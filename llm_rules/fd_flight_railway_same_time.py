from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent, FlightEvent, RailwayEvent
from expensecbr.fde import create_time_window_rule
from datetime import datetime, timedelta


def detect_flight_railway_same_time(rule, events, context):
    """
    Detect when a user has overlapping flight and railway journeys in the same time period.
    
    This rule identifies physically impossible travel patterns where a single user
    claims to be both on a flight and on a train during overlapping time windows,
    which clearly indicates fraudulent activity or expense data entry errors.
    """
    # Filter to include only flight and railway events
    flight_events = [e for e in events if isinstance(e, FlightEvent)]
    railway_events = [e for e in events if isinstance(e, RailwayEvent)]
    
    # If there are no flight or railway events, nothing to check
    if not flight_events or not railway_events:
        return False
    
    # Group events by user
    events_by_user = {}
    
    # Organize flight events by user
    for event in flight_events:
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = {"flights": [], "railways": []}
        events_by_user[event.user_id]["flights"].append(event)
    
    # Organize railway events by user
    for event in railway_events:
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = {"flights": [], "railways": []}
        events_by_user[event.user_id]["railways"].append(event)
    
    suspicious_patterns = []
    
    # For each user, check for overlapping flight and railway journeys
    for user_id, user_events in events_by_user.items():
        # Skip if user doesn't have both flight and railway events
        if not user_events["flights"] or not user_events["railways"]:
            continue
        
        # Check each flight against each railway journey for overlap
        for flight in user_events["flights"]:
            for railway in user_events["railways"]:
                # Check if the time windows overlap
                if rule.do_time_intervals_overlap(flight.time_window, railway.time_window):
                    # Calculate the overlap duration in hours
                    overlap_hours = rule.get_overlap_duration(
                        flight.time_window, railway.time_window, unit="hours"
                    )
                    
                    # Only consider as suspicious if overlap is substantial (more than 30 minutes)
                    # This helps avoid false positives from minor scheduling overlaps
                    if overlap_hours >= 0.5:
                        # Add to suspicious patterns
                        suspicious_patterns.append({
                            "primary_event_id": flight.event_id,  # Use flight as primary event
                            "secondary_event_id": railway.event_id,
                            "user_id": user_id,
                            "user_name": flight.user_name,
                            "department": flight.department,
                            "overlap_hours": overlap_hours,
                            "flight": {
                                "event_id": flight.event_id,
                                "flight_no": flight.flight_no,
                                "airline": flight.airline,
                                "from_city": flight.from_location.city if flight.from_location else "Unknown",
                                "to_city": flight.to_location.city if flight.to_location else "Unknown",
                                "start_time": flight.time_window.earliest_start,
                                "end_time": flight.time_window.latest_end,
                                "amount": flight.amount,
                                "cabin_class": flight.cabin_class
                            },
                            "railway": {
                                "event_id": railway.event_id,
                                "train_number": railway.train_number,
                                "train_type": railway.train_type,
                                "from_city": railway.from_location.city if railway.from_location else "Unknown",
                                "to_city": railway.to_location.city if railway.to_location else "Unknown",
                                "start_time": railway.time_window.earliest_start,
                                "end_time": railway.time_window.latest_end,
                                "amount": railway.amount,
                                "seat_class": railway.seat_class
                            }
                        })
    
    return suspicious_patterns if suspicious_patterns else False


def format_flight_railway_same_time_alert(rule, events, extra_data, context):
    """Format alert details for overlapping flight and railway journeys"""
    # Get user information
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    
    # Get flight information
    flight_info = extra_data.get("flight", {})
    flight_no = flight_info.get("flight_no", "Unknown")
    airline = flight_info.get("airline", "Unknown")
    flight_from = flight_info.get("from_city", "Unknown")
    flight_to = flight_info.get("to_city", "Unknown")
    flight_amount = flight_info.get("amount", 0.0)
    flight_start = flight_info.get("start_time")
    flight_end = flight_info.get("end_time")
    cabin_class = flight_info.get("cabin_class", "Unknown")
    
    # Get railway information
    railway_info = extra_data.get("railway", {})
    train_number = railway_info.get("train_number", "Unknown")
    train_type = railway_info.get("train_type", "Unknown")
    railway_from = railway_info.get("from_city", "Unknown")
    railway_to = railway_info.get("to_city", "Unknown")
    railway_amount = railway_info.get("amount", 0.0)
    railway_start = railway_info.get("start_time")
    railway_end = railway_info.get("end_time")
    seat_class = railway_info.get("seat_class", "Unknown")
    
    # Format the times
    flight_start_str = flight_start.strftime("%Y-%m-%d %H:%M") if flight_start else "Unknown"
    flight_end_str = flight_end.strftime("%Y-%m-%d %H:%M") if flight_end else "Unknown"
    railway_start_str = railway_start.strftime("%Y-%m-%d %H:%M") if railway_start else "Unknown"
    railway_end_str = railway_end.strftime("%Y-%m-%d %H:%M") if railway_end else "Unknown"
    
    # Get overlap information
    overlap_hours = extra_data.get("overlap_hours", 0)
    total_amount = flight_amount + railway_amount
    
    # Create a title with key information
    title = f"Simultaneous Flight and Railway Travel: {flight_no} and {train_number} ({total_amount:.2f} yuan)"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} has claimed expenses for both a flight "
        f"and a railway journey with a time overlap of {overlap_hours:.1f} hours.\n\n"
        f"This is physically impossible as a person cannot be on both a flight and a train simultaneously.\n\n"
        f"Flight Details:\n"
        f"- Flight Number: {flight_no} ({airline})\n"
        f"- Route: {flight_from} to {flight_to}\n"
        f"- Time: {flight_start_str} to {flight_end_str}\n"
        f"- Cabin Class: {cabin_class}\n"
        f"- Amount: {flight_amount:.2f} yuan\n\n"
        f"Railway Details:\n"
        f"- Train Number: {train_number} ({train_type})\n"
        f"- Route: {railway_from} to {railway_to}\n"
        f"- Time: {railway_start_str} to {railway_end_str}\n"
        f"- Seat Class: {seat_class}\n"
        f"- Amount: {railway_amount:.2f} yuan\n\n"
        f"Possible Explanations:\n"
        f"- Multiple different people using the same employee ID\n"
        f"- One booking was for a colleague but claimed under own ID\n"
        f"- Data entry errors in journey dates/times\n"
        f"- Booking was made but the journey was canceled without updating records\n\n"
        f"Recommended Action: Verify actual journeys with tickets and boarding passes"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
flight_railway_same_time_rule = create_time_window_rule(
    rule_id="FD-FLIGHT-RAILWAY-SAME-TIME",
    title="Flight and Railway Same Time",
    description="Detects when a user has overlapping flight and railway journeys in the same time period, which is physically impossible",
    severity="high",
    event_types=["FlightEvent", "RailwayEvent"],
    detect_fn=detect_flight_railway_same_time,
    format_alert_fn=format_flight_railway_same_time_alert,
    window_days=3  # Look at events within 3 days
)