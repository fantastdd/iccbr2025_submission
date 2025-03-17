from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent
from expensecbr.fde import create_individual_rule
from datetime import datetime, timezone


def detect_time_travel_expense(rule, events, context):
    """
    Detect when an expense has a submission time that is earlier than the event's
    actual occurrence time, which is a logical contradiction (time travel).
    
    This rule identifies expense records where the system submission timestamp
    predates the earliest possible start time of the expense event itself,
    which indicates either fraudulent manipulation or data entry errors.
    """
    # We expect a list with a single event due to our individual grouping strategy
    if not events or len(events) != 1:
        return False

    event = events[0]
    
    # Get the event's time window - we'll use earliest_start as the event occurrence time
    event_time = event.time_window.earliest_start
    
    # Get the submission timestamp from the event
    # Note: This assumes there's a submission_time attribute on the event.
    # If this doesn't exist in your actual implementation, you would need to
    # modify this to use the appropriate field that stores submission time.
    submission_time = getattr(event, "submission_time", None)
    
    # If submission time is not available, we can't check for time travel
    if not submission_time:
        return False
    
    # Calculate the time difference in hours (can be negative if submission is before event)
    time_diff_hours = rule.time_difference(
        submission_time, event_time, unit="hours"
    )
    
    # If submission time is earlier than event time (negative difference),
    # flag as suspicious time travel
    if time_diff_hours < 0:
        # Get the absolute time difference for reporting
        abs_diff_hours = abs(time_diff_hours)
        days = int(abs_diff_hours // 24)
        hours = int(abs_diff_hours % 24)
        
        return {
            "primary_event_id": event.event_id,
            "event_type": event.__class__.__name__,
            "user_id": event.user_id,
            "user_name": event.user_name,
            "event_time": event_time,
            "submission_time": submission_time,
            "time_diff_hours": time_diff_hours,
            "time_diff_formatted": f"{days} days, {hours} hours" if days > 0 else f"{hours} hours",
            "amount": event.amount,
            "event_location": event.location.city if event.location else "Unknown"
        }

    return False


def format_time_travel_expense_alert(rule, events, extra_data, context):
    """Format alert details for time travel expense submissions"""
    # Get the primary event
    event = events[0]
    
    # Get information from extra_data
    event_type = extra_data.get("event_type", "Expense")
    event_time = extra_data.get("event_time")
    submission_time = extra_data.get("submission_time")
    time_diff_formatted = extra_data.get("time_diff_formatted", "Unknown")
    
    # Format the timestamps
    event_time_str = event_time.strftime("%Y-%m-%d %H:%M") if event_time else "Unknown"
    submission_time_str = submission_time.strftime("%Y-%m-%d %H:%M") if submission_time else "Unknown"
    
    # Get location and amount
    location = extra_data.get("event_location", "Unknown")
    amount = extra_data.get("amount", 0.0)
    
    # Create the alert message
    title = f"Time Travel Expense: Submitted {time_diff_formatted} before occurrence"
    
    details = (
        f"User {event.user_name} ({event.user_id}) from {event.department} submitted an expense "
        f"that was recorded in the system before the event actually occurred.\n\n"
        f"Event Type: {event_type}\n"
        f"Event Time: {event_time_str}\n"
        f"Submission Time: {submission_time_str}\n"
        f"Time Discrepancy: Submitted {time_diff_formatted} before the event occurred\n"
        f"Location: {location}\n"
        f"Amount: {amount:.2f} yuan\n\n"
        f"This logical contradiction may indicate:\n"
        f"- Manually modified timestamps to bypass submission deadlines\n"
        f"- System clock inconsistencies between client and server\n"
        f"- Incorrect event date entry (future date entered by mistake)\n"
        f"- Planned expenses submitted before the event actually occurred\n\n"
        f"Recommended Action: Verify the actual date of the expense occurrence and whether "
        f"the event actually took place as claimed."
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
time_travel_expense_rule = create_individual_rule(
    rule_id="FD-TIME-TRAVEL-EXPENSE",
    title="Time Travel Expense",
    description="Detects expenses that were submitted before they actually occurred, which is a logical contradiction",
    severity="medium",
    event_types=["TrajectoryEvent"],  # Apply to all event types
    detect_fn=detect_time_travel_expense,
    format_alert_fn=format_time_travel_expense_alert,
)