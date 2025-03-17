from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, time
from expensecbr.base import TrajectoryEvent, TaxiEvent, Location
from expensecbr.fde import create_individual_rule

def detect_commute_taxi_usage(rule, events, context):
    """
    Detect employees using taxis for regular commuting between home and workplace during workdays.
    
    This rule identifies when employees take taxis between their home and office locations
    during normal business hours on workdays, which violates company policy. However,
    late night commutes (after 10:30 PM) are exempt from this policy.
    """
    # We expect a single event due to our individual grouping strategy
    if not events or len(events) != 1:
        return False

    event = events[0]
    
    # Skip if not a taxi event
    if not isinstance(event, TaxiEvent):
        return False
    
    # Get user's home and work locations from context
    home_locations = context.get("default_home_locations", {})
    work_locations = context.get("default_work_locations", {})
    office_locations = context.get("default_office_locations", {})
    
    # Get user's home location
    home_location = home_locations.get(event.user_id)
    if not home_location:
        return False  # Skip if we don't know user's home
    
    # Get user's work location
    work_location = work_locations.get(event.user_id)
    if not work_location:
        # Try to get from office locations based on user's city
        user_city = None
        if home_location and home_location.city:
            user_city = home_location.city
        
        if user_city and user_city in office_locations:
            work_location = office_locations[user_city]
        
        if not work_location:
            return False  # Skip if we don't know user's workplace
    
    # Get from and to locations from taxi event
    from_location = getattr(event, "from_location", None)
    to_location = getattr(event, "to_location", None)
    
    if not from_location or not to_location:
        return False  # Skip if taxi locations are missing
    
    # Check if this is a commute trip (home to work or work to home)
    is_home_to_work = is_same_location(from_location, home_location) and is_same_location(to_location, work_location)
    is_work_to_home = is_same_location(from_location, work_location) and is_same_location(to_location, home_location)
    
    is_commute_trip = is_home_to_work or is_work_to_home
    
    if not is_commute_trip:
        return False  # Not a commute trip
    
    # Check if this is during a workday (Monday-Friday)
    event_time = event.time_window.earliest_start
    is_workday = event_time.weekday() < 5  # 0-4 are Monday-Friday
    
    if not is_workday:
        return False  # Weekend travel is allowed
    
    # Check if this is during normal business hours
    # Late night commutes (after 10:30 PM) are allowed by policy
    late_night_threshold = time(22, 30)  # 10:30 PM
    is_late_night = event_time.time() >= late_night_threshold
    
    if is_late_night:
        return False  # Late night commutes are allowed
    
    # Get working hours from context
    working_hours = context.get("working_hours", {"start": 9, "end": 18})
    work_start_hour = working_hours.get("start", 9)
    work_end_hour = working_hours.get("end", 18)
    
    # Allow commuting slightly before work starts or after work ends
    # (within 90 minutes buffer)
    buffer_minutes = 90
    
    work_start_time = time(work_start_hour, 0)
    work_end_time = time(work_end_hour, 0)
    
    # Calculate buffer times
    before_work_earliest = datetime.combine(event_time.date(), work_start_time) - timedelta(minutes=buffer_minutes)
    after_work_latest = datetime.combine(event_time.date(), work_end_time) + timedelta(minutes=buffer_minutes)
    
    # Convert to time objects for comparison
    before_work_earliest_time = before_work_earliest.time()
    after_work_latest_time = after_work_latest.time()
    
    # Check if the commute is during typical commuting hours
    event_time_only = event_time.time()
    
    # Morning commute: from buffer before work start time
    is_morning_commute = before_work_earliest_time <= event_time_only <= work_start_time
    
    # Evening commute: until buffer after work end time
    is_evening_commute = work_end_time <= event_time_only <= after_work_latest_time
    
    # If it's neither a morning nor evening commute during buffer times, it's policy violation
    if not (is_morning_commute or is_evening_commute):
        return False  # Not during typical commuting hours
    
    # At this point, we have a commute trip violation
    return {
        "primary_event_id": event.event_id,
        "user_id": event.user_id,
        "user_name": event.user_name,
        "department": event.department,
        "date": event_time.strftime("%Y-%m-%d"),
        "time": event_time.strftime("%H:%M"),
        "amount": event.amount,
        "direction": "Home to Work" if is_home_to_work else "Work to Home",
        "from_location": from_location.full_address if hasattr(from_location, "full_address") else str(from_location),
        "to_location": to_location.full_address if hasattr(to_location, "full_address") else str(to_location),
    }


def is_same_location(loc1, loc2):
    """
    Check if two locations are the same or very close.
    First tries is_same_city method, falls back to string comparison if needed.
    """
    # Check if locations have is_same_city method
    if hasattr(loc1, "is_same_city") and callable(loc1.is_same_city):
        return loc1.is_same_city(loc2)
    
    # Fall back to comparing city strings
    if hasattr(loc1, "city") and hasattr(loc2, "city"):
        return loc1.city == loc2.city
    
    # Last resort: string comparison of the whole object
    return str(loc1) == str(loc2)


def format_commute_taxi_alert(rule, events, extra_data, context):
    """Format alert details for the commute taxi usage rule"""
    # Get data from extra_data
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    date = extra_data.get("date")
    time = extra_data.get("time")
    amount = extra_data.get("amount")
    direction = extra_data.get("direction")
    from_location = extra_data.get("from_location")
    to_location = extra_data.get("to_location")
    
    title = f"Policy Violation: Commute Taxi Usage ({direction})"
    
    details = (
        f"User {user_name} ({user_id}) from {department} used a taxi for regular commuting "
        f"on {date} at {time}, which violates company policy.\n\n"
        f"Trip details:\n"
        f"- Direction: {direction}\n"
        f"- From: {from_location}\n"
        f"- To: {to_location}\n"
        f"- Cost: {amount} yuan\n\n"
        f"Company policy prohibits using taxis for regular commuting between home and "
        f"workplace during normal business hours on workdays. Only late night commutes "
        f"(after 10:30 PM) are permitted."
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
commute_taxi_rule = create_individual_rule(
    rule_id="FD-POLICY-COMMUTE-TRIP",
    title="Workday Commute Taxi Usage",
    description="Detects when employees use taxis for regular commuting between home and workplace during workdays, which violates company policy. Late night commutes (after 10:30 PM) are allowed.",
    severity="medium",
    event_types=["TaxiEvent"],
    detect_fn=detect_commute_taxi_usage,
    format_alert_fn=format_commute_taxi_alert
)