from typing import Dict, Any, List

from expensecbr.base import TrajectoryEvent, TaxiEvent
from expensecbr.fde import create_individual_rule


def detect_commute_trip(rule, events, context):
    """
    Detect taxi rides used for daily commuting during workdays, which violates company policy.
    
    This rule identifies taxi rides that:
    1. Occur on workdays (Monday to Friday)
    2. Take place during typical commuting hours (morning: 7:00-10:00, evening: 17:00-19:30)
    3. Travel between an employee's home and workplace
    
    Exception: Late night commutes (after 22:30/10:30 PM) are allowed by company policy.
    
    Returns details of violations or False if no violation is detected.
    """
    # We expect a list with a single event due to our individual grouping strategy
    if not events or len(events) != 1:
        return False

    event = events[0]

    # Skip if not a taxi event or is self-paid
    if not isinstance(event, TaxiEvent) or getattr(event, "is_self_paid", False):
        return False

    # Get user's home and work locations from context
    home_locations = context.get("default_home_locations", {})
    work_locations = context.get("default_work_locations", {})
    
    # Get locations for this user
    user_id = event.user_id
    home_location = home_locations.get(user_id)
    work_location = work_locations.get(user_id)
    
    # Skip if we don't have both home and work locations for this user
    if not home_location or not work_location:
        return False

    # Get event start time (prefer exact time if available)
    event_time = event.time_window.earliest_start
    
    # Skip weekend rides
    if rule.is_weekend(event_time):
        return False
    
    # Allow late night commutes (after 10:30 PM / 22:30) as per policy
    hour = event_time.hour + event_time.minute / 60.0
    if hour >= 22.5:  # 22:30 in decimal hours
        return False
    
    # Check if this is during commute hours
    is_morning_commute = rule.is_within_time_range(event_time, 7.0, 10.0)
    is_evening_commute = rule.is_within_time_range(event_time, 17.0, 19.5)
    
    if not (is_morning_commute or is_evening_commute):
        return False

    # Check if this is a ride between home and work
    from_location = event.from_location
    to_location = event.to_location
    
    # Check for home-to-work commute (typically morning)
    home_to_work = (
        (rule.is_within_distance(from_location, home_location, 1.0) and
         rule.is_within_distance(to_location, work_location, 1.0))
    )
    
    # Check for work-to-home commute (typically evening)
    work_to_home = (
        (rule.is_within_distance(from_location, work_location, 1.0) and
         rule.is_within_distance(to_location, home_location, 1.0))
    )
    
    # If this is a commute trip, return the details
    if home_to_work or work_to_home:
        commute_type = "home-to-work" if home_to_work else "work-to-home"
        commute_period = "morning" if is_morning_commute else "evening"
        
        return {
            "primary_event_id": event.event_id,
            "user_id": event.user_id,
            "user_name": event.user_name,
            "amount": event.amount,
            "time": event_time,
            "commute_type": commute_type,
            "commute_period": commute_period,
            "from_location": from_location.city + (f", {from_location.specific_location}" if from_location.specific_location else ""),
            "to_location": to_location.city + (f", {to_location.specific_location}" if to_location.specific_location else ""),
            "home_city": home_location.city,
            "work_city": work_location.city,
        }

    return False


def format_commute_trip_alert(rule, events, extra_data, context):
    """Format alert details for commute trip violations"""
    # We know there's only one event in the list due to our grouping strategy
    event = events[0]

    # Get data from extra_data
    commute_type = extra_data.get("commute_type", "unknown")
    commute_period = extra_data.get("commute_period", "unknown")
    
    # Format time
    time_str = extra_data.get("time").strftime("%Y-%m-%d %H:%M")
    day_of_week = extra_data.get("time").strftime("%A")
    
    # Format locations
    from_loc = extra_data.get("from_location", "Unknown")
    to_loc = extra_data.get("to_location", "Unknown")

    # Create meaningful alert title
    title = f"Policy Violation: {commute_period.capitalize()} Commute Taxi ({event.amount:.2f} yuan)"

    details = (
        f"User {event.user_name} ({event.user_id}) used a taxi for commuting on {time_str} ({day_of_week}), "
        f"which violates company policy. The taxi was used for a {commute_type} trip during {commute_period} commute hours.\n\n"
        f"Details:\n"
        f"- From: {from_loc}\n"
        f"- To: {to_loc}\n"
        f"- Amount: {event.amount:.2f} yuan\n"
        f"- Date & Time: {time_str}\n\n"
        f"According to company policy, employees should not use taxi services for regular commuting between "
        f"home and workplace during standard hours. Only commute trips after 10:30 PM are allowed for reimbursement."
    )

    return {"title": title, "details": details}


# Create the rule using the factory function
commute_trip_rule = create_individual_rule(
    rule_id="FD-POLICY-COMMUTE-TRIP",
    title="Workday Commute Taxi Usage",
    description="Detects when employees use taxis for regular commuting between home and workplace during workdays, which violates company policy. Late night commutes (after 10:30 PM) are allowed.",
    severity="medium",
    event_types=["TaxiEvent"],
    detect_fn=detect_commute_trip,
    format_alert_fn=format_commute_trip_alert,
)