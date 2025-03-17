from typing import Dict, Any, List
from expensecbr.base import (
    TrajectoryEvent,
    DailyCheckInEvent,
    FlightEvent,
    RailwayEvent,
    TaxiEvent,
    FuelEvent
)
from expensecbr.fde import create_daily_rule
from datetime import datetime, timedelta


def detect_checkin_without_transportation(rule, events, context):
    """
    Detect when a user checks in at a location far from their previous day's activity
    location (especially in another city) without any transportation expense records
    to explain the movement.
    
    This rule identifies physically suspicious patterns where a user appears to have
    teleported to a different location without any record of how they traveled there.
    """
    # Filter for check-in events
    checkin_events = [e for e in events if isinstance(e, DailyCheckInEvent)]
    
    # Filter for transportation events
    transport_events = [
        e for e in events if isinstance(e, (FlightEvent, RailwayEvent, TaxiEvent, FuelEvent))
    ]
    
    # If there are no check-in events or fewer than 2, nothing to check
    if len(checkin_events) < 2:
        return False
    
    suspicious_patterns = []
    
    # Group events by user
    events_by_user = {}
    for event in events:
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = {
                "checkins": [],
                "transport": []
            }
        
        if isinstance(event, DailyCheckInEvent):
            events_by_user[event.user_id]["checkins"].append(event)
        elif isinstance(event, (FlightEvent, RailwayEvent, TaxiEvent, FuelEvent)):
            events_by_user[event.user_id]["transport"].append(event)
    
    # For each user, check for suspicious check-in patterns
    for user_id, user_events in events_by_user.items():
        checkins = user_events["checkins"]
        transport = user_events["transport"]
        
        # Sort check-ins by date
        checkins.sort(key=lambda x: x.time_window.earliest_start)
        
        # Compare consecutive check-ins
        for i in range(1, len(checkins)):
            current_checkin = checkins[i]
            previous_checkin = checkins[i-1]
            
            # Get check-in cities
            current_city = current_checkin.location.city if current_checkin.location else None
            previous_city = previous_checkin.location.city if previous_checkin.location else None
            
            # Skip if we can't determine either city
            if not current_city or not previous_city:
                continue
            
            # Skip if both check-ins are in the same city (not suspicious)
            if current_city == previous_city:
                continue
            
            # Calculate days between check-ins
            days_between = (current_checkin.time_window.earliest_start.date() - 
                           previous_checkin.time_window.earliest_start.date()).days
            
            # Skip if check-ins are more than 3 days apart (too much time for reliable detection)
            if days_between > 3:
                continue
            
            # Establish the time window to look for transportation
            transport_start = previous_checkin.time_window.earliest_start
            transport_end = current_checkin.time_window.earliest_start
            
            # Check if there's any transportation record that explains the movement
            has_transportation = False
            relevant_transport = []
            
            for t_event in transport:
                # Skip if outside the relevant time window
                if (t_event.time_window.latest_end < transport_start or 
                    t_event.time_window.earliest_start > transport_end):
                    continue
                
                # For different transport types, check if they explain the city change
                if isinstance(t_event, (FlightEvent, RailwayEvent)):
                    # Check if the transport goes from previous city to current city
                    from_city = t_event.from_location.city if t_event.from_location else None
                    to_city = t_event.to_location.city if t_event.to_location else None
                    
                    if (from_city and to_city and 
                        from_city == previous_city and to_city == current_city):
                        has_transportation = True
                        relevant_transport.append(t_event)
                
                elif isinstance(t_event, TaxiEvent):
                    # For taxis, check if they're in either city (could be at origin or destination)
                    taxi_city = t_event.location.city if t_event.location else None
                    
                    if taxi_city and (taxi_city == previous_city or taxi_city == current_city):
                        # For this to count, check that the taxi is to/from a transport hub
                        from_loc = t_event.from_location.specific_location if t_event.from_location else ""
                        to_loc = t_event.to_location.specific_location if t_event.to_location else ""
                        
                        # Check if location mentions airport, station, etc.
                        transport_keywords = ["机场", "airport", "站", "station", "terminal"]
                        is_transport_related = any(keyword in from_loc or keyword in to_loc 
                                                 for keyword in transport_keywords)
                        
                        if is_transport_related:
                            has_transportation = True
                            relevant_transport.append(t_event)
                
                elif isinstance(t_event, FuelEvent):
                    # For fuel, it could be anywhere along the journey
                    fuel_city = t_event.location.city if t_event.location else None
                    
                    # Count fuel purchases in either city or in between
                    if fuel_city:
                        has_transportation = True
                        relevant_transport.append(t_event)
            
            # If no transport was found but cities are different, flag as suspicious
            if not has_transportation:
                # Calculate distance between cities if possible
                distance = None
                if previous_checkin.location and current_checkin.location:
                    distance = rule.get_distance(previous_checkin.location, current_checkin.location)
                
                suspicious_patterns.append({
                    "primary_event_id": current_checkin.event_id,
                    "user_id": user_id,
                    "user_name": current_checkin.user_name,
                    "department": current_checkin.department,
                    "previous_checkin_id": previous_checkin.event_id,
                    "previous_city": previous_city,
                    "current_city": current_city,
                    "previous_time": previous_checkin.time_window.earliest_start,
                    "current_time": current_checkin.time_window.earliest_start,
                    "days_between": days_between,
                    "distance_km": distance,
                    "previous_location": (
                        previous_checkin.location.full_address 
                        if previous_checkin.location and previous_checkin.location.full_address 
                        else previous_city
                    ),
                    "current_location": (
                        current_checkin.location.full_address 
                        if current_checkin.location and current_checkin.location.full_address 
                        else current_city
                    ),
                    "previous_activity": getattr(previous_checkin, "activity_type", "Check-in"),
                    "current_activity": getattr(current_checkin, "activity_type", "Check-in"),
                })
    
    return suspicious_patterns if suspicious_patterns else False


def format_checkin_without_transportation_alert(rule, events, extra_data, context):
    """Format alert details for check-in without transportation"""
    # Get user information
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    
    # Get check-in information
    previous_city = extra_data.get("previous_city", "Unknown")
    current_city = extra_data.get("current_city", "Unknown")
    previous_time = extra_data.get("previous_time")
    current_time = extra_data.get("current_time")
    days_between = extra_data.get("days_between", 0)
    distance_km = extra_data.get("distance_km")
    previous_location = extra_data.get("previous_location", previous_city)
    current_location = extra_data.get("current_location", current_city)
    previous_activity = extra_data.get("previous_activity", "Check-in")
    current_activity = extra_data.get("current_activity", "Check-in")
    
    # Format dates
    previous_time_str = previous_time.strftime("%Y-%m-%d %H:%M") if previous_time else "Unknown"
    current_time_str = current_time.strftime("%Y-%m-%d %H:%M") if current_time else "Unknown"
    
    # Format distance information
    distance_str = f"{distance_km:.1f} km" if distance_km is not None else "Unknown"
    
    # Create title with key information
    title = f"Check-in Without Transportation: {previous_city} to {current_city} ({days_between} days)"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} checked in at {current_city} on {current_time_str}, "
        f"just {days_between} day(s) after checking in at {previous_city} on {previous_time_str}.\n\n"
        f"No transportation expense records (flights, trains, taxis, or fuel) were found to explain "
        f"how they traveled between these cities, which are {distance_str} apart.\n\n"
        f"Previous Check-in Details:\n"
        f"- Location: {previous_location}\n"
        f"- Time: {previous_time_str}\n"
        f"- Activity: {previous_activity}\n\n"
        f"Current Check-in Details:\n"
        f"- Location: {current_location}\n"
        f"- Time: {current_time_str}\n"
        f"- Activity: {current_activity}\n\n"
        f"Possible Explanations:\n"
        f"- Transportation expenses were not submitted for reimbursement\n"
        f"- Transportation was booked and expensed by another employee\n"
        f"- Employee used personal transportation with no reimbursable expenses\n"
        f"- Check-in was performed remotely or by another person using the employee's credentials\n"
        f"- Date/time or location data entry errors\n\n"
        f"Recommended Action: Verify the employee's actual travel between these locations"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
checkin_without_transportation_rule = create_daily_rule(
    rule_id="FD-CHECKIN-WITHOUT-TRANSPORTATION",
    title="Check-in Without Transportation",
    description="Detects when a user checks in at a location far from their previous day's activity without any transportation expense records",
    severity="medium",
    event_types=["DailyCheckInEvent", "FlightEvent", "RailwayEvent", "TaxiEvent", "FuelEvent"],
    detect_fn=detect_checkin_without_transportation,
    format_alert_fn=format_checkin_without_transportation_alert,
)