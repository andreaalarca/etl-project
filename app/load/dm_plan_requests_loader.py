"""
Adapter for DM Plan Requests Loader to match factory pattern expectations.
This adapts the datamart loader to the expected app.load.*_loader format.
"""

# Import the actual implementation from datamart
from datamart.load.dm_plan_requests_loader import DMPLANREQUESTSLoader

# Re-export with the expected class name for the factory pattern
# The factory expects: {title_case_table_name}Loader
# For dm_plan_requests: DmPlanRequestsLoader
DmPlanRequestsLoader = DMPLANREQUESTSLoader


def execute() -> str:
    """
    Execute the dm_plan_requests data loading process.
    Delegates to the datamart loader's execute method.

    Returns:
        str: Status message indicating success and details
    """
    # Delegate to the actual implementation in datamart
    loader = DMPLANREQUESTSLoader()
    return loader.execute()