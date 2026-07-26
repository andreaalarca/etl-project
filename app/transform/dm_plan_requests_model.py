"""
Adapter for DM Plan Requests Model to match factory pattern expectations.
This adapts the app model to the expected app.transform.*_transformer format.
"""

# Import the actual implementation from app model
from app.model.dm_plan_requests_model import DmPlanRequestsModel

# Re-export with the expected class name for the factory pattern
# The factory expects: {title_case_table_name}Transformer
# For dm_plan_requests: DmPlanRequestsTransformer
# Note: We're using the transformer adapter slot for the model component
DmPlanRequestsTransformer = DmPlanRequestsModel


def execute() -> str:
    """
    Execute the dm_plan_requests data modeling process.
    Delegates to the app model's execute method.

    Returns:
        str: Status message indicating success and details
    """
    # Delegate to the actual implementation in app model
    model = DmPlanRequestsModel()
    return model.execute()