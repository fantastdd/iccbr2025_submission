from typing import Dict, Any, List
from expensecbr.base import (
    TrajectoryEvent, 
    HotelEvent, 
    FlightEvent, 
    RailwayEvent,
    TaxiEvent,
    FuelEvent
)
from expensecbr.fde import create_time_window_rule
from datetime import datetime, timedelta


def detect_hotel_stay_no_arrival(rule, events, context):
    """
    Detect when a user has a hotel stay far from their home location without any
    transportation records showing how they arrived at that city.
    
    This rule identifies suspicious patterns where a user claims hotel expenses in
    a city different from their home/work location, but there are no corresponding
    transportation expenses (flights, trains, taxis, fuel) that would explain how
    they traveled to that city.
    """
    # Filter for hotel events
    hotel_events = [e for e in events if isinstance(e, HotelEvent)]
    
    # If there are no hotel events, nothing to check
    if not hotel_events:
        return False
    
    # Filter for transportation events
    transport_events = [
        e for e in events if isinstance(e, (FlightEvent, RailwayEvent, TaxiEvent, FuelEvent))
    ]
    
    # Get home and work locations from context
    home_locations = context.get("default_home_locations", {})
    work_locations = context.get("default_work_locations", {})
    
    suspicious_patterns = []
    
    # For each hotel stay, check if there's a matching arrival transportation record
    for hotel_event in hotel_events:
        user_id = hotel_event.user_id
        hotel_city = hotel_event.location.city if hotel_event.location else None
        
        # Skip if we can't determine hotel city
        if not hotel_city:
            continue
        
        # Get user's home and work locations
        home_loc = home_locations.get(user_id)
        work_loc = work_locations.get(user_id)
        
        home_city = home_loc.city if home_loc else None
        work_city = work_loc.city if work_loc else None
        
        # Skip if hotel is in home or work city (not suspicious)
        if (home_city and hotel_city == home_city) or (work_city and hotel_city == work_city):
            continue
        
        # We now have a hotel stay in a different city - check for transportation
        
        # Define the time window to search for transportation
        # Look for transport events from 3 days before check-in until check-in time
        search_start = hotel_event.time_window.earliest_start - timedelta(days=3)
        search_end = hotel_event.time_window.earliest_start
        
        # Flag to track if we found matching transportation
        has_arrival_transport = False
        
        # Check each transport event for a match
        for transport in transport_events:
            # Skip if not the same user
            if transport.user_id != user_id:
                continue
            
            # Skip if the transport event is after hotel check-in
            if transport.time_window.latest_end > hotel_event.time_window.earliest_start:
                continue
            
            # Skip if the transport event is too old (more than 3 days before)
            if transport.time_window.earliest_start < search_start:
                continue
            
            # For each transport type, check if it arrives at the hotel city
            destination_city = None
            
            if isinstance(transport, (FlightEvent, RailwayEvent)):
                if hasattr(transport, "to_location") and transport.to_location:
                    destination_city = transport.to_location.city
                    
            elif isinstance(transport, TaxiEvent):
                if hasattr(transport, "to_location") and transport.to_location:
                    destination_city = transport.to_location.city
                    
            elif isinstance(transport, FuelEvent):
                # For fuel events, check if it's in the hotel city (fueling at destination)
                if transport.location:
                    destination_city = transport.location.city
            
            # If this transport arrives at the hotel city, mark as not suspicious
            if destination_city and destination_city == hotel_city:
                has_arrival_transport = True
                break
        
        # If no matching transportation was found, mark as suspicious
        if not has_arrival_transport:
            residential_city = home_city or work_city or "Unknown"
            suspicious_patterns.append({
                "primary_event_id": hotel_event.event_id,
                "user_id": user_id,
                "user_name": hotel_event.user_name,
                "department": hotel_event.department,
                "hotel_name": hotel_event.hotel_name,
                "hotel_city": hotel_city,
                "residential_city": residential_city,
                "check_in": hotel_event.time_window.earliest_start,
                "check_out": hotel_event.time_window.latest_end,
                "amount": hotel_event.amount,
                "guest_name": hotel_event.guest_name,
                "guest_type": hotel_event.guest_type,
                "room_type": hotel_event.room_type
            })
    
    return suspicious_patterns if suspicious_patterns else False


def format_hotel_stay_no_arrival_alert(rule, events, extra_data, context):
    """Format alert details for hotel stay without arrival transportation"""
    # Get primary event ID from extra_data
    primary_event_id = extra_data.get("primary_event_id")
    
    # Find the primary event in the events list
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    if not primary_event:
        # Fallback if primary event not found
        return {
            "title": "Hotel Stay Without Arrival Transportation",
            "details": "Insufficient data available."
        }
    
    # Extract user information
    user_name = extra_data.get("user_name", primary_event.user_name)
    user_id = extra_data.get("user_id", primary_event.user_id)
    department = extra_data.get("department", primary_event.department)
    
    # Extract hotel details
    hotel_name = extra_data.get("hotel_name", getattr(primary_event, "hotel_name", "Unknown"))
    hotel_city = extra_data.get("hotel_city", "Unknown")
    residential_city = extra_data.get("residential_city", "Unknown")
    check_in = extra_data.get("check_in")
    check_out = extra_data.get("check_out")
    amount = extra_data.get("amount", primary_event.amount)
    guest_name = extra_data.get("guest_name", getattr(primary_event, "guest_name", "Unknown"))
    guest_type = extra_data.get("guest_type", getattr(primary_event, "guest_type", "Unknown"))
    room_type = extra_data.get("room_type", getattr(primary_event, "room_type", "Unknown"))
    
    # Format dates
    check_in_str = check_in.strftime("%Y-%m-%d") if check_in else "Unknown"
    check_out_str = check_out.strftime("%Y-%m-%d") if check_out else "Unknown"
    
    # Calculate stay duration
    stay_duration = "Unknown"
    if check_in and check_out:
        days = (check_out - check_in).total_seconds() / (24 * 3600)
        stay_duration = f"{days:.1f} days"
    
    # Create title with key details
    title = f"Hotel Stay Without Arrival Transportation: {hotel_city} ({amount:.2f} yuan)"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} claimed expenses for a "
        f"hotel stay at {hotel_name} in {hotel_city} from {check_in_str} to {check_out_str}, "
        f"but no transportation records were found to explain how they traveled to {hotel_city}.\n\n"
        f"This is suspicious because:\n"
        f"- {hotel_city} is different from the user's residential city ({residential_city})\n"
        f"- No flights, trains, taxis, or fuel records show travel to {hotel_city} before the stay\n"
        f"- The time gap suggests transportation costs may be missing or filed under a different ID\n\n"
        f"Hotel Stay Details:\n"
        f"- Hotel: {hotel_name}\n"
        f"- City: {hotel_city}\n"
        f"- Check-in: {check_in_str}\n"
        f"- Check-out: {check_out_str}\n"
        f"- Duration: {stay_duration}\n"
        f"- Amount: {amount:.2f} yuan\n"
        f"- Guest Name: {guest_name}\n"
        f"- Guest Type: {guest_type}\n"
        f"- Room Type: {room_type}\n\n"
        f"Possible Explanations:\n"
        f"- Transportation expenses were not submitted for reimbursement\n"
        f"- Transportation was booked and expensed by another employee\n"
        f"- Transportation expenses were filed under a different ID or time period\n"
        f"- Hotel booking was made for someone else but claimed under this ID\n\n"
        f"Recommended Action: Verify how the employee traveled to this location"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_stay_no_arrival_rule = create_time_window_rule(
    rule_id="FD-HOTEL-STAY-NO-ARRIVAL",
    title="Hotel Stay Without Arrival Transportation",
    description="Detects when a user has hotel stays far from their residential location without any transportation records showing how they arrived at that city",
    severity="medium",
    event_types=["HotelEvent", "FlightEvent", "RailwayEvent", "TaxiEvent", "FuelEvent"],
    detect_fn=detect_hotel_stay_no_arrival,
    format_alert_fn=format_hotel_stay_no_arrival_alert,
    window_days=3  # Look at events within 3 days
)