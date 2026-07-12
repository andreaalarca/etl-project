#!/usr/bin/env python3
"""
DAG runner for the ETL pipeline.

- Reads a dependency map (YAML or Python dict) from config/
- Topologically sorts the tasks
- Executes each task in order:
    * pull_<name>   -> runs the extractor for <name> (just extracts and discards)
    * preprocess_<name> -> runs the full ETL script (extract+transform+load)
    * otherwise     -> tries app.preprocess.<task>.py (fallback)
"""

import importlib
import sys
from pathlib import Path
from typing import Dict, List

# Ensure the project root is on sys.path so we can import app.* modules
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # .../etl-pipeline
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Try to load yaml; if not available, fall back to a plain Python module.
try:
    import yaml  # type: ignore
    HAS_YAML = True
except Exception:  # pragma: no cover
    HAS_YAML = False

# For parallel execution
from concurrent.futures import ProcessPoolExecutor, as_completed


CONFIG_DIR = PROJECT_ROOT / "config"


def load_dependencies() -> Dict[str, List[str]]:
    """
    Return a dict: {task_name: [list_of_dependency_task_names]}
    """
    yaml_path = CONFIG_DIR / "dependencies.yaml"
    py_path = CONFIG_DIR / "dependencies.py"

    if yaml_path.exists() and HAS_YAML:
        with yaml_path.open("rt", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError("dependencies.yaml must be a mapping")
        return {str(k): [str(x) for x in v] for k, v in data.items()}

    if py_path.exists():
        spec = importlib.util.spec_from_file_location("deps", py_path)
        module = importlib.util.module_from_spec(spec)  # type: ignore
        assert spec and spec.loader  # for mypy
        spec.loader.exec_module(module)  # type: ignore
        if not hasattr(module, "DEPENDENCIES"):
            raise AttributeError("dependencies.py must expose DEPENDENCIES dict")
        deps = getattr(module, "DEPENDENCIES")
        if not isinstance(deps, dict):
            raise ValueError("DEPENDENCIES must be a dict")
        return {str(k): [str(x) for x in v] for k, v in deps.items()}

    raise FileNotFoundError(
        "Neither dependencies.yaml nor dependencies.py found in config/"
    )


def topological_sort(deps: Dict[str, List[str]]) -> List[str]:
    """
    Kahn's algorithm on a dependency map where deps[node] = list of prerequisites.
    Returns a list of task names in execution order.
    Raises ValueError if a cycle is detected.
    """
    # Build adjacency list (outgoing edges) and indegree count
    incoming: Dict[str, int] = {node: 0 for node in deps}
    outgoing: Dict[str, List[str]] = {node: [] for node in deps}

    for node, prereqs in deps.items():
        # Ensure prereqs are known tasks
        for p in prereqs:
            if p not in incoming:
                raise ValueError(f"Dependency {p!r} of {node!r} not defined as a task")
            incoming[node] += 1
            outgoing[p].append(node)

    # Queue of nodes with zero incoming edges (no prerequisites)
    zero_indegree = [n for n, deg in incoming.items() if deg == 0]
    order: List[str] = []

    while zero_indegree:
        n = zero_indegree.pop(0)
        order.append(n)
        for m in outgoing.get(n, []):
            incoming[m] -= 1
            if incoming[m] == 0:
                zero_indegree.append(m)

    if len(order) != len(deps):
        # There's a cycle
        remaining = [n for n, d in incoming.items() if d > 0]
        raise ValueError(f"Circular dependency detected involving: {remaining}")

    return order


def run_pull_task(task_name: str) -> bool:
    """
    task_name examples: pull_customer, pull_construction_plan_types
    Maps to: app.pull.<name>_extractor
    We'll call the extract() function if available, otherwise fall back to legacy detection.
    """
    # Remove 'pull_' prefix
    prefix = "pull_"
    if not task_name.startswith(prefix):
        print(f"[ERROR] run_pull_task called on non-pull task: {task_name}")
        return False
    base = task_name[len(prefix):]  # e.g., "customer"
    module_name = f"app.pull.{base}_extractor"
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        print(f"[ERROR] Could not import module {module_name!r}: {e}")
        return False

    # Try to find a simple extract() function
    extract_fn = None
    if hasattr(module, "extract") and callable(getattr(module, "extract")):
        extract_fn = getattr(module, "extract")
    else:
        # Fallback: look for extract_<base>_from_raw
        fn_name = f"extract_{base}_from_raw"
        if hasattr(module, fn_name) and callable(getattr(module, fn_name)):
            extract_fn = getattr(module, fn_name)
        else:
            # Fallback: look for a class <Base>Extractor with extract method
            class_candidates = [
                f"{base.capitalize()}Extractor",
                f"{base.title().replace('_', '')}Extractor",
                f"{base.upper()}Extractor",
            ]
            for class_name in class_candidates:
                if hasattr(module, class_name):
                    cls = getattr(module, class_name)
                    if hasattr(cls, "extract"):
                        # Create a lambda that instantiates and calls extract
                        extract_fn = lambda cls=cls: cls().extract()
                        break
            # Final fallback: look for any callable named extract_<base>
            if extract_fn is None:
                fn_name2 = f"extract_{base}"
                if hasattr(module, fn_name2) and callable(getattr(module, fn_name2)):
                    extract_fn = getattr(module, fn_name2)

    if extract_fn is None:
        print(f"[ERROR] Could not find an extract function in module {module_name!r}")
        return False

    try:
        # Call the extract function – it may need raw_dir from env; our extractors default to ./data/raw
        result = extract_fn()
        # We don't need to store the result; just ensure it didn't raise.
        print(f"[INFO] Pull task {task_name}: extracted data (type: {type(result).__name__})")
        return True
    except Exception as e:
        print(f"[ERROR] Exception while extracting in {task_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_preprocess_task(task_name: str) -> bool:
    """
    task_name examples: preprocess_customer, preprocess_construction_plan_types
    Maps to: app.preprocess.<name> (without the preprocess_)
    """
    prefix = "preprocess_"
    if not task_name.startswith(prefix):
        print(f"[ERROR] run_preprocess_task called on non-preprocess task: {task_name}")
        return False
    base = task_name[len(prefix):]  # e.g., "customer"
    module_path = f"app.preprocess.{base}"
    try:
        module = importlib.import_module(module_path)
    except Exception as e:
        print(f"[ERROR] Could not import module {module_path!r}: {e}")
        return False

    if not hasattr(module, "main"):
        print(f"[ERROR] Module {module_path!r} has no callable main()")
        return False

    try:
        result = module.main()  # type: ignore
        if result is None:
            print(f"[INFO] {task_name}: main() returned None (treated as success)")
            return True
        return bool(result)
    except Exception as e:
        print(f"[ERROR] Exception while running {task_name}.main(): {e}")
        import traceback
        traceback.print_exc()
        return False


def run_task(task_name: str) -> bool:
    """
    Dispatch based on task prefix.
    """
    if task_name.startswith("pull_"):
        return run_pull_task(task_name)
    elif task_name.startswith("preprocess_"):
        return run_preprocess_task(task_name)
    else:
        # Fallback: try app.preprocess.<task>
        module_path = f"app.preprocess.{task_name}"
        try:
            module = importlib.import_module(module_path)
        except Exception as e:
            print(f"[ERROR] Could not import module {module_path!r}: {e}")
            return False
        if not hasattr(module, "main"):
            print(f"[ERROR] Module {module_path!r} has no callable main()")
            return False
        try:
            result = module.main()
            if result is None:
                return True
            return bool(result)
        except Exception as e:
            print(f"[ERROR] Exception while running {task_name}.main(): {e}")
            import traceback
            traceback.print_exc()
            return False


def main() -> int:
    print("Loading dependency map ...")
    deps = load_dependencies()
    print(f"Loaded {len(deps)} tasks: {list(deps.keys())}")

    # Build dependency graph for level computation
    incoming: Dict[str, int] = {node: 0 for node in deps}
    outgoing: Dict[str, List[str]] = {node: [] for node in deps}

    for node, prereqs in deps.items():
        for p in prereqs:
            if p not in incoming:
                raise ValueError(f"Dependency {p!r} of {node!r} not defined as a task")
            incoming[node] += 1
            outgoing[p].append(node)

    # Find initial tasks with no dependencies
    zero_indegree = [n for n, deg in incoming.items() if deg == 0]

    levels: List[List[str]] = []
    # Process levels (Kahn's algorithm level-by-level)
    while zero_indegree:
        current_level = list(zero_indegree)  # copy
        levels.append(current_level)
        next_zero: List[str] = []
        for task in current_level:
            for dependent in outgoing.get(task, []):
                incoming[dependent] -= 1
                if incoming[dependent] == 0:
                    next_zero.append(dependent)
        zero_indegree = next_zero

    # Check for cycles
    total_tasks_in_levels = sum(len(level) for level in levels)
    if total_tasks_in_levels != len(deps):
        remaining = [n for n, d in incoming.items() if d > 0]
        raise ValueError(f"Circular dependency detected involving: {remaining}")

    print("Execution levels (tasks that can run in parallel):")
    for i, level in enumerate(levels):
        print(f"  Level {i}: {level}")

    # No CLI filters: run all levels in parallel (default)
    failed: List[str] = []
    # Execute each level
    for level_idx, level_tasks in enumerate(levels):
        print(f"\n{'='*60}")
        print(f"EXECUTING LEVEL {level_idx} ({len(level_tasks)} task(s) in parallel)")
        print(f"{'='*60}")

        # Use ProcessPoolExecutor to avoid import lock issues in threads
        with ProcessPoolExecutor(max_workers=len(level_tasks)) as executor:
            future_to_task = {
                executor.submit(run_task, task): task for task in level_tasks
            }
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    ok = future.result()
                except Exception as e:
                    print(f"[ERROR] Exception while running {task}: {e}")
                    import traceback
                    traceback.print_exc()
                    ok = False
                if ok:
                    print(f"[INFO] Task {task} completed successfully")
                else:
                    print(f"[ERROR] Task {task} FAILED")
                    failed.append(task)

        # Stop on first failure in any level (fail-fast)
        if failed:
            print(f"\n[ERROR] Stopping execution due to failure(s) in level {level_idx}")
            break

    print("\n" + "="*60)
    print("EXECUTION SUMMARY")
    print("="*60)
    if not failed:
        print("All tasks completed successfully.")
        return 0
    else:
        print(f"Failed tasks: {failed}")
        return 1


if __name__ == "__main__":
    sys.exit(main())