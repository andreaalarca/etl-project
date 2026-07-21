# ETL Flow Explanation (Construction Plan Types Job)

This document explains the end‑to‑end flow of the **construction_plan_types_job.py** job, focusing on how chunking interacts with staging and archiving, and clarifies the archiving behavior.

---  

## 1. High‑level Pipeline  

```
Extractor  →  Preprocessor  →  Transformer  →  Loader
   │            │            │            │
   ▼            ▼            ▼            ▼
Source file  Cleaned DF   Transformed DF  Loaded into Warehouse
                                                            │
                                                            ▼
                                                     Archive (staging → archive)
```

The **Job orchestrator** (`app/jobs/construction_plan_types_job.py`) orchestrates the four stages:

1. **Creates** the extractor, preprocessor, transformer, and loader objects.  
2. **Pulls** data from the extractor – either a single `pandas.DataFrame` (no chunking) or a generator of `DataFrame`s (when `chunksize` > 0).  
3. **Passes a writer object** to the preprocessor so cleaned data can be appended to a **single** Parquet staging file.  
4. **Sends** each transformed chunk to the loader for bulk insertion.  
5. **After all chunks** are processed, archives the single staging file (or moves it to an error directory on failure).

---

## 2. Extractor Output  

| `chunksize` argument | Return type of `extract_construction_plan_types(chunksize=…)` |
|----------------------|--------------------------------------------------------------|
| **`None`** (default) | A single `pandas.DataFrame` containing the whole file. |
| **Integer > 0**      | A **generator** yielding `pandas.DataFrame` objects, each ≤ `chunksize` rows. |

*The extractor only reads the source file; it never writes to disk.*

---

## 3. Writer (Staging File Handling)

* The **preprocessor** (`ConstructionPlanTypesPreprocessor`) receives a `writer` argument.  
* **First call**: creates a **Parquet writer** pointed at `self.staging_dir` (e.g., `data/staging/construction_plan_types_20260721.parquet`).  
* **Subsequent calls**: receives the same writer, appends the cleaned chunk, and returns the updated writer.  
* After the final chunk, the job’s `finally` block calls `writer.close()` to finalize the file.

**Result:** Whether the data arrived as one DataFrame or many chunks, **exactly one** Parquet file ends up at `self.staging_dir`.

---

## 4. Loader Responsibilities  

* `ConstructionPlanTypesLoader.__init__` receives the target schema, table name, and `batch_size` from the job configuration.  
* Its `load(df)` method receives a **transformed** `DataFrame` (either the whole dataset or a chunk) and inserts it into the warehouse using:  

  1. **Primary path** – `psycopg2.extras.execute_values` (high‑performance bulk insert).  
  2. **Fallback path** – SQLAlchemy `to_sql` with `method="multi"` if the psycopg2 path fails.  

*The loader never touches the staging file; it works solely with the in‑memory DataFrame supplied to it.*

---

## 5. Archiving Step  

After the `try/except` block finishes (whether successfully or via an exception that is re‑raised only after writer handling), the job executes:

```python
if writer is not None:
    logger.info("Construction Plan Types Job", "Load",
                f"Archiving staging files from {loader.staging_dir}")
    loader.archive(loader.staging_dir)
```

* `loader.archive(source_file: Path)` moves the file from `source_file` (the staging path) to a **timestamped subdirectory** under the configured archive root:

```
<archive_root>/<timestamp>/construction_plan_types_20260721.parquet
```

* Because there is **only one** staging file (see §3), the archive step moves **that single file**.  
* The archive filename **remains unchanged**; only its directory gets a timestamp, preserving traceability.

### Archiving Behavior Summary

| Scenario                               | Staging file produced                                   | Archive result                                                                 |
|----------------------------------------|----------------------------------------------------------|--------------------------------------------------------------------------------|
| **No chunking** (`chunksize=None`)     | One Parquet file containing the whole dataset.           | Moved once to `<archive>/<timestamp>/construction_plan_types_<date>.parquet`. |
| **Chunked processing** (`chunksize=N`) | One Parquet file that grows incrementally as each cleaned chunk is appended. | Same single file moved once after the last chunk has been processed.          |
| **Error during processing**            | Writer may have written some chunks; job moves the **partially written** file to the error directory (`_move_source_file_to_error`) **instead** of archiving it. | No archive; the source file is quarantined for inspection.                    |

---

## 6. Diagram of the Flow with Chunking  

```
Source file (CSV)
        │
Extractor (generator of chunks) ──► ──► ──► ──► ──► ──► ──► ──►
        │                           │   │   │   │   │   │   │
        ▼                           ▼   ▼   ▼   ▼   ▼   ▼   ▼
Preprocessor (receives writer)   Chunk‑1 Chunk‑2 … Chunk‑k
        │                           │   │   │   │   │   │   │
        ▼                           ▼   ▼   ▼   ▼   ▼   ▼   ▼
Transformer                       ↓   ↓   ↓   ↓   ↓   ↓   ↓   ↓
        │                         (cleaned chunks)
        ▼                           ▼   ▼   ▼   ▼   ▼   ▼   ▼
Loader                            ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼
        │                         Inserted into warehouse
        ▼                           ▼   ▼   ▼   ▼   ▼   ▼   ▼
   (after loop) writer.close()   │   │   │   │   │   │   │   │
        │                         │   │   │   │   │   │   │   │
        ▼                         ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼
   Archive step ──────────────────► Move staging.parquet → archive/<ts>/staging.parquet
```

---

## 7. Key Take‑aways  

* **Chunking does NOT create multiple staging/archive files** – the design intentionally writes *all* cleaned data to a **single** Parquet file (`staging_path`).  
* Consequently, the **archive step always moves exactly one file**, regardless of `chunksize`.  
* If you ever need **per‑chunk archiving** (e.g., to keep intermediate snapshots), you would need to modify the job to call `loader.archive` inside the chunk loop or change the preprocessor to write a new file each iteration.  
* The current implementation is optimal for most ETL workloads: a single archive file simplifies downstream auditing while still allowing **high‑performance bulk inserts** via chunked reading to keep memory usage low.  

---

*Thank you for reading!*