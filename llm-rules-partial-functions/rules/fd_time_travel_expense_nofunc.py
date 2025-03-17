from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent
from expensecbr.fde import create_individual_rule

def detect_time_travel_expense(rule, events, context):
    """
    Detect expenses that have been submitted for reimbursement before they actually occurred.
    
    This rule identifies the logical contradiction when a reimbursement submission timestamp
    is earlier than the event's actual occurrence time, which is physically impossible and
    indicates either fraudulent activity or data entry errors.
    """
    # We expect a single event due to individual grouping strategy
    if not events or len(events) != 1:
        return False
    
    event = events[0]
    
    # Get the submission timestamp (when the expense was submitted for reimbursement)
    # This would typically be stored in a metadata field or similar
    submission_time = getattr(event, "submission_time", None)
    
    # If no submission time is available, we can't perform the check
    if not submission_time:
        return False
    
    # Get the event's time window for when the expense actually occurred
    event_start_time = event.time_window.earliest_start
    
    # Check if submission time is before the event's start time
    if submission_time < event_start_time:
        # Calculate how far in advance the submission was made
        time_difference = (event_start_time - submission_time).total_seconds() / 3600  # hours
        
        # If the difference is negligible (e.g., a few seconds due to system timing issues),
        # we should ignore it to prevent false positives
        if time_difference < 0.01:  # Less than 36 seconds
            return False
        
        return {
            "primary_event_id": event.event_id,
            "user_id": event.user_id,
            "user_name": event.user_name,
            "department": event.department,
            "event_type": type(event).__name__,
            "submission_time": submission_time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_time": event_start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "time_difference_hours": round(time_difference, 2),
            "amount": getattr(event, "amount", None)
        }
    
    return False


def format_time_travel_alert(rule, events, extra_data, context):
    """Format alert details for the time travel expense rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    event_type = extra_data.get("event_type", "Expense")
    submission_time = extra_data.get("submission_time")
    event_time = extra_data.get("event_time")
    time_difference = extra_data.get("time_difference_hours")
    amount = extra_data.get("amount")
    
    # Find the primary event
    primary_event_id = extra_data.get("primary_event_id")
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    
    # Get event-specific details
    event_details = ""
    if primary_event:
        # Add location information if available
        if hasattr(primary_event, "location") and primary_event.location:
            location = primary_event.location
            city = location.city if hasattr(location, "city") else "Unknown"
            specific_loc = location.specific_location if hasattr(location, "specific_location") else ""
            event_details += f"Location: {city} {specific_loc}\n"
        
        # Add event-specific fields based on event type
        if hasattr(primary_event, "remark") and primary_event.remark:
            event_details += f"Remark: {primary_event.remark}\n"
    
    title = f"Time Paradox: Expense Submitted {time_difference} Hours Before Occurrence"
    
    details = (
        f"User {user_name} ({user_id}) from {department} submitted a reimbursement request "
        f"for a {event_type} before the expense actually occurred.\n\n"
        f"Submission time: {submission_time}\n"
        f"Actual event time: {event_time}\n"
        f"Time difference: {time_difference} hours\n"
    )
    
    if amount is not None:
        details += f"Amount: {amount} yuan\n"
    
    if event_details:
        details += f"\nAdditional details:\n{event_details}"
    
    details += (
        f"\nThis activity is suspicious because it's impossible to submit a reimbursement "
        f"for an expense that hasn't happened yet. This could indicate:"
        f"\n- Fraudulent backdating of expenses"
        f"\n- Submission of fabricated expenses"
        f"\n- Severe data entry or system timestamp errors"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
time_travel_expense_rule = create_individual_rule(
    rule_id="FD-TIME-TRAVEL-EXPENSE",
    title="Time Travel Expense",
    description="Detects expenses submitted for reimbursement before they actually occurred",
    severity="high",
    event_types=["TrajectoryEvent"],  # Apply to all trajectory events
    detect_fn=detect_time_travel_expense,
    format_alert_fn=format_time_travel_alert
)