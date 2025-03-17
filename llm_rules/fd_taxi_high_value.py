from typing import Dict, Any, List

from expensecbr.base import TrajectoryEvent, TaxiEvent
from expensecbr.fde import create_individual_rule


def detect_high_value_taxi(rule, events, context):
    """
    Detect unusually expensive taxi rides that may indicate fraud.

    This rule examines individual taxi events and flags those with costs exceeding
    a threshold amount (50 yuan by default).
    """
    # We expect a list with a single event due to our individual grouping strategy
    if not events or len(events) != 1:
        return False

    event = events[0]

    # Skip if not a taxi event or is self-paid
    if not isinstance(event, TaxiEvent) or getattr(event, "is_self_paid", False):
        return False

    # Get threshold from context or use default
    threshold = context.get("taxi_high_value_threshold", 50.0)

    # Check if the amount exceeds the threshold
    if event.amount > threshold:
        return {
            "primary_event_id": event.event_id,
            "amount": event.amount,
            "threshold": threshold,
            "excess_amount": event.amount - threshold,
        }

    return False


def format_high_value_alert(rule, events, extra_data, context):
    """Format alert details for the high-value taxi rule"""
    # We know there's only one event in the list due to our grouping strategy
    event = events[0]

    # Format locations
    from_location = getattr(event, "from_location", None)
    to_location = getattr(event, "to_location", None)

    from_loc_str = from_location.full_address if from_location else "Unknown"
    to_loc_str = to_location.full_address if to_location else "Unknown"

    # Format time
    time_str = event.time_window.exact_start_time.strftime("%Y-%m-%d %H:%M")

    details = (
        f"User {event.user_name} ({event.user_id}) took an expensive taxi ride on {time_str} "
        f"from {from_loc_str} to {to_loc_str} costing {event.amount} yuan. "
        f"This exceeds the threshold of {extra_data.get('threshold', 50.0)} yuan by "
        f"{extra_data.get('excess_amount', 0.0):.2f} yuan."
    )

    return {"title": f"High-Value Taxi Ride: {event.amount} yuan", "details": details}


# Create the rule using the factory function
high_value_taxi_rule = create_individual_rule(
    rule_id="FD-TAXI-HIGH-VALUE",
    title="High-Value Taxi Rides",
    description="Detects unusually expensive taxi rides that may indicate fraud",
    severity="medium",
    event_types=["TaxiEvent"],
    detect_fn=detect_high_value_taxi,
    format_alert_fn=format_high_value_alert,
)
