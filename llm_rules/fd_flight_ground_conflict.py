from typing import Dict, Any, List
from expensecbr.base import (
    TrajectoryEvent,
    FlightEvent,
    TaxiEvent,
    FuelEvent,
    DailyCheckInEvent
)
from expensecbr.fde import create_time_window_rule
from datetime import datetime, timedelta


def detect_flight_ground_conflict(rule, events, context):
    """
    Detect when a user claims ground transportation expenses during flight times.
    
    This rule identifies physically impossible scenarios where a user submits expenses
    for ground activities (taxi rides, fuel purchases, check-ins) that overlap with
    times when they were supposedly on a flight.
    """
    # Filter for flight events
    flight_events = [e for e in events if isinstance(e, FlightEvent)]
    
    # If there are no flight events, nothing to check
    if not flight_events:
        return False
    
    # Filter for ground events
    ground_events = [
        e for e in events if isinstance(e, (TaxiEvent, FuelEvent, DailyCheckInEvent))
    ]
    
    # If there are no ground events, nothing to check
    if not ground_events:
        return False
    
    suspicious_patterns = []
    
    # For each flight, check for overlapping ground activities
    for flight in flight_events:
        user_id = flight.user_id
        
        # Get flight details
        flight_from = flight.from_location.city if flight.from_location else None
        flight_to = flight.to_location.city if flight.to_location else None
        
        # Skip if we can't determine flight cities
        if not flight_from or not flight_to:
            continue
        
        # Get flight time window - add buffer time for boarding and deplaning
        flight_time = flight.time_window
        
        # Create a buffer for realistic flight coverage (including airport procedures)
        # 1.5 hours before departure and 1 hour after arrival
        buffered_start = flight_time.earliest_start - timedelta(hours=1.5)
        buffered_end = flight_time.latest_end + timedelta(hours=1)
        
        # List to collect conflicting ground events
        conflicting_events = []
        
        # Check each ground event by the same user for overlap
        for ground in ground_events:
            # Skip if not the same user
            if ground.user_id != user_id:
                continue
            
            # Get ground event city
            ground_city = ground.location.city if ground.location else None
            
            # Skip if we can't determine ground event city
            if not ground_city:
                continue
            
            # Skip if ground event is in departure or arrival city (could be before/after flight)
            if ground_city == flight_from or ground_city == flight_to:
                # For events in flight cities, we need to check time carefully
                # Only flag if the event is during the actual flight time (not buffer)
                if rule.do_time_intervals_overlap(ground.time_window, flight_time):
                    # Calculate overlap with the actual flight (not buffered time)
                    overlap_minutes = rule.get_overlap_duration(
                        ground.time_window, flight_time, unit="minutes"
                    )
                    
                    # Only consider as suspicious if overlap is substantial (more than 15 minutes)
                    if overlap_minutes >= 15:
                        conflicting_events.append({
                            "event_id": ground.event_id,
                            "event_type": ground.__class__.__name__,
                            "city": ground_city,
                            "start_time": ground.time_window.earliest_start,
                            "end_time": ground.time_window.latest_end,
                            "amount": ground.amount,
                            "overlap_minutes": overlap_minutes
                        })
            # If ground event is in different city than both departure and arrival,
            # it's suspicious regardless of buffer time
            else:
                # Check if the ground event overlaps with the buffered flight time
                if rule.do_time_intervals_overlap(ground.time_window, flight.time_window):
                    # Calculate overlap with flight window
                    overlap_minutes = rule.get_overlap_duration(
                        ground.time_window, flight.time_window, unit="minutes"
                    )
                    
                    # Only consider as suspicious if overlap is substantial (more than 15 minutes)
                    if overlap_minutes >= 15:
                        conflicting_events.append({
                            "event_id": ground.event_id,
                            "event_type": ground.__class__.__name__,
                            "city": ground_city,
                            "start_time": ground.time_window.earliest_start,
                            "end_time": ground.time_window.latest_end,
                            "amount": ground.amount,
                            "overlap_minutes": overlap_minutes
                        })
        
        # If we found conflicting ground events, add to suspicious patterns
        if conflicting_events:
            suspicious_patterns.append({
                "primary_event_id": flight.event_id,
                "user_id": user_id,
                "user_name": flight.user_name,
                "department": flight.department,
                "flight_no": flight.flight_no,
                "airline": flight.airline,
                "flight_from": flight_from,
                "flight_to": flight_to,
                "flight_start": flight_time.earliest_start,
                "flight_end": flight_time.latest_end,
                "flight_amount": flight.amount,
                "cabin_class": flight.cabin_class,
                "conflicting_events": conflicting_events
            })
    
    return suspicious_patterns if suspicious_patterns else False


def format_flight_ground_conflict_alert(rule, events, extra_data, context):
    """Format alert details for flight and ground activity conflict"""
    # Get user information
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    
    # Get flight information
    flight_no = extra_data.get("flight_no", "Unknown")
    airline = extra_data.get("airline", "Unknown")
    flight_from = extra_data.get("flight_from", "Unknown")
    flight_to = extra_data.get("flight_to", "Unknown")
    flight_start = extra_data.get("flight_start")
    flight_end = extra_data.get("flight_end")
    flight_amount = extra_data.get("flight_amount", 0.0)
    cabin_class = extra_data.get("cabin_class", "Unknown")
    
    # Get conflicting events
    conflicting_events = extra_data.get("conflicting_events", [])
    
    # Format dates
    flight_start_str = flight_start.strftime("%Y-%m-%d %H:%M") if flight_start else "Unknown"
    flight_end_str = flight_end.strftime("%Y-%m-%d %H:%M") if flight_end else "Unknown"
    
    # Calculate total amount of conflicting events
    total_conflict_amount = sum(event.get("amount", 0) for event in conflicting_events)
    
    # Create title with key information
    title = f"Flight and Ground Activity Conflict: {flight_no} ({flight_from} to {flight_to})"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} claimed expenses for ground activities "
        f"during a flight, which is physically impossible.\n\n"
        f"Flight Details:\n"
        f"- Flight: {flight_no} ({airline})\n"
        f"- Route: {flight_from} to {flight_to}\n"
        f"- Time: {flight_start_str} to {flight_end_str}\n"
        f"- Cabin Class: {cabin_class}\n"
        f"- Amount: {flight_amount:.2f} yuan\n\n"
        f"Conflicting Ground Activities During Flight:\n"
    )
    
    # Add details for each conflicting event
    for i, event in enumerate(conflicting_events, 1):
        event_type = event.get("event_type", "Unknown")
        city = event.get("city", "Unknown")
        start_time = event.get("start_time")
        end_time = event.get("end_time")
        amount = event.get("amount", 0.0)
        overlap_minutes = event.get("overlap_minutes", 0)
        
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M") if start_time else "Unknown"
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M") if end_time else "Unknown"
        
        details += (
            f"{i}. {event_type} in {city}\n"
            f"   Time: {start_time_str} to {end_time_str}\n"
            f"   Overlap: {overlap_minutes:.0f} minutes\n"
            f"   Amount: {amount:.2f} yuan\n\n"
        )
    
    details += (
        f"Total conflicting expenses: {total_conflict_amount:.2f} yuan\n\n"
        f"Possible Explanations:\n"
        f"- Flight or ground event timestamps are incorrect\n"
        f"- Ground expenses were claimed by someone else using this employee's ID\n"
        f"- Canceled flight with expenses still submitted\n"
        f"- Multiple people sharing the same employee ID\n\n"
        f"Recommended Action: Verify the actual flight and ground transportation records"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
flight_ground_conflict_rule = create_time_window_rule(
    rule_id="FD-FLIGHT-GROUND-CONFLICT",
    title="Flight and Ground Activity Conflict",
    description="Detects when a user claims ground transportation expenses during flight times",
    severity="high",
    event_types=["FlightEvent", "TaxiEvent", "FuelEvent", "DailyCheckInEvent"],
    detect_fn=detect_flight_ground_conflict,
    format_alert_fn=format_flight_ground_conflict_alert,
    window_days=2  # Look at events within 2 days
)