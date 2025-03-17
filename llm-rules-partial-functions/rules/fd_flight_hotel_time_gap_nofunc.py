from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent, HotelEvent, FlightEvent
from expensecbr.fde import create_time_window_rule

def detect_flight_hotel_time_gap(rule, events, context):
    """
    Detect logical time conflicts between flight arrivals and hotel check-ins.
    
    This rule identifies two types of conflicts:
    1. Check-in occurs too soon after flight arrival (not enough time to travel from airport to hotel)
    2. Check-in occurs too long after flight arrival (unexplained gap between arrival and check-in)
    
    Both scenarios may indicate either fraudulent expense claims, data entry errors, or
    missing transportation/accommodation records.
    """
    if not events:
        return False
    
    # Group events by user_id
    events_by_user = {}
    for event in events:
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = []
        events_by_user[event.user_id].append(event)
    
    fraud_instances = []
    
    for user_id, user_events in events_by_user.items():
        # Filter for hotel and flight events
        hotel_events = [e for e in user_events if isinstance(e, HotelEvent)]
        flight_events = [e for e in user_events if isinstance(e, FlightEvent)]
        
        # Skip if user doesn't have both hotel and flight events
        if not hotel_events or not flight_events:
            continue
        
        # Sort events by time
        hotel_events.sort(key=lambda e: e.time_window.earliest_start)
        flight_events.sort(key=lambda e: e.time_window.earliest_start)
        
        # Check each flight arrival against subsequent hotel check-ins
        for flight in flight_events:
            # Get flight arrival details
            arrival_location = getattr(flight, "arrival_location", None)
            arrival_city = arrival_location.city if arrival_location else None
            
            if not arrival_city:
                continue
                
            # Get flight arrival time (prefer exact time, fall back to latest end time)
            arrival_time = flight.time_window.exact_end_time or flight.time_window.latest_end
            arrival_date = arrival_time.date()
            
            # Find hotel check-ins on the same day or the day after the flight arrival
            relevant_hotels = []
            for hotel in hotel_events:
                check_in_time = hotel.time_window.exact_start_time or hotel.time_window.earliest_start
                check_in_date = check_in_time.date()
                
                # Consider check-ins on the same day or next day
                date_diff = (check_in_date - arrival_date).days
                if 0 <= date_diff <= 1:
                    relevant_hotels.append(hotel)
            
            for hotel in relevant_hotels:
                # Get hotel details
                hotel_location = hotel.location
                hotel_city = hotel_location.city if hotel_location else None
                
                if not hotel_city:
                    continue
                    
                # Get hotel check-in time
                check_in_time = hotel.time_window.exact_start_time or hotel.time_window.earliest_start
                
                # Skip if cities don't match - covered by another rule
                if hotel_city != arrival_city:
                    continue
                
                # Calculate time gap between flight arrival and hotel check-in
                time_gap_hours = (check_in_time - arrival_time).total_seconds() / 3600
                
                # Case 1: Check-in occurs too soon after arrival (physically impossible)
                # Minimum reasonable time includes deplaning, baggage claim, and transit to hotel
                min_reasonable_gap_hours = 1.0  # 1 hour as minimum reasonable time
                
                if 0 <= time_gap_hours < min_reasonable_gap_hours:
                    fraud_instances.append({
                        "primary_event_id": hotel.event_id,
                        "flight_event_id": flight.event_id,
                        "user_id": user_id,
                        "user_name": hotel.user_name,
                        "department": hotel.department,
                        "conflict_type": "too_soon",
                        "city": hotel_city,
                        "arrival_time": arrival_time.strftime("%Y-%m-%d %H:%M"),
                        "check_in_time": check_in_time.strftime("%Y-%m-%d %H:%M"),
                        "time_gap_hours": round(time_gap_hours, 2),
                        "min_reasonable_gap": min_reasonable_gap_hours,
                        "flight_number": getattr(flight, "flight_number", "Unknown"),
                        "hotel_name": getattr(hotel, "specific_location", "Unknown"),
                        "hotel_amount": getattr(hotel, "amount", None),
                        "flight_amount": getattr(flight, "amount", None)
                    })
                
                # Case 2: Check-in occurs very late after arrival (suspiciously long gap)
                # Maximum reasonable time before checking in (without another event explaining the gap)
                max_reasonable_gap_hours = 8.0  # 8 hours as maximum reasonable gap
                
                # For overnight flights, allow longer gap if arrival is early morning and check-in is afternoon
                is_early_morning_arrival = arrival_time.hour < 7
                is_afternoon_checkin = check_in_time.hour >= 14
                
                # Adjust max gap for early morning arrivals
                if is_early_morning_arrival and is_afternoon_checkin:
                    max_reasonable_gap_hours = 12.0  # Allow longer gap for early arrivals
                
                # Check if there's a suspiciously long gap
                if time_gap_hours > max_reasonable_gap_hours:
                    # Check if there are any other events in between that might explain the gap
                    has_intermediate_events = False
                    for event in user_events:
                        if (event != flight and event != hotel and 
                                arrival_time < event.time_window.earliest_start < check_in_time):
                            has_intermediate_events = True
                            break
                    
                    # Only flag if there are no intermediate events explaining the gap
                    if not has_intermediate_events:
                        fraud_instances.append({
                            "primary_event_id": hotel.event_id,
                            "flight_event_id": flight.event_id,
                            "user_id": user_id,
                            "user_name": hotel.user_name,
                            "department": hotel.department,
                            "conflict_type": "too_late",
                            "city": hotel_city,
                            "arrival_time": arrival_time.strftime("%Y-%m-%d %H:%M"),
                            "check_in_time": check_in_time.strftime("%Y-%m-%d %H:%M"),
                            "time_gap_hours": round(time_gap_hours, 2),
                            "max_reasonable_gap": max_reasonable_gap_hours,
                            "flight_number": getattr(flight, "flight_number", "Unknown"),
                            "hotel_name": getattr(hotel, "specific_location", "Unknown"),
                            "hotel_amount": getattr(hotel, "amount", None),
                            "flight_amount": getattr(flight, "amount", None)
                        })
    
    return fraud_instances if fraud_instances else False


def format_flight_hotel_time_gap_alert(rule, events, extra_data, context):
    """Format alert details for the flight-hotel time gap rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    conflict_type = extra_data.get("conflict_type")
    city = extra_data.get("city")
    arrival_time = extra_data.get("arrival_time")
    check_in_time = extra_data.get("check_in_time")
    time_gap_hours = extra_data.get("time_gap_hours")
    flight_number = extra_data.get("flight_number")
    hotel_name = extra_data.get("hotel_name")
    hotel_amount = extra_data.get("hotel_amount")
    flight_amount = extra_data.get("flight_amount")
    
    # Format different titles and details based on conflict type
    if conflict_type == "too_soon":
        min_gap = extra_data.get("min_reasonable_gap")
        
        title = f"Impossible Check-in: Too Soon After Flight Arrival ({time_gap_hours} hours)"
        
        details = (
            f"User {user_name} ({user_id}) from {department} checked into a hotel "
            f"too soon after their flight arrived in {city}.\n\n"
            f"Flight {flight_number} arrival: {arrival_time}\n"
            f"Hotel check-in at {hotel_name}: {check_in_time}\n"
            f"Time gap: {time_gap_hours} hours\n\n"
            f"This time gap is less than the minimum reasonable time ({min_gap} hours) needed to "
            f"deplane, collect baggage, and travel to the hotel. This physical impossibility "
            f"suggests either inaccurate time reporting or fraudulent expense claims."
        )
    
    elif conflict_type == "too_late":
        max_gap = extra_data.get("max_reasonable_gap")
        
        title = f"Suspicious Gap: {time_gap_hours} Hours Between Flight and Hotel"
        
        details = (
            f"User {user_name} ({user_id}) from {department} has a suspiciously long gap "
            f"between flight arrival and hotel check-in in {city}.\n\n"
            f"Flight {flight_number} arrival: {arrival_time}\n"
            f"Hotel check-in at {hotel_name}: {check_in_time}\n"
            f"Time gap: {time_gap_hours} hours\n\n"
            f"This time gap exceeds the maximum reasonable time ({max_gap} hours) between "
            f"arrival and check-in, with no intermediate events to explain the gap. This could "
            f"indicate missing expense records or fraudulent claims for either the flight or hotel."
        )
    
    else:
        title = "Flight-Hotel Time Conflict"
        details = f"User {user_name} ({user_id}) from {department} has a logical conflict between flight arrival and hotel check-in times."
    
    # Add cost information if available
    cost_details = ""
    if hotel_amount is not None:
        cost_details += f"Hotel cost: {hotel_amount} yuan\n"
    if flight_amount is not None:
        cost_details += f"Flight cost: {flight_amount} yuan\n"
    
    if cost_details:
        details += f"\nExpense details:\n{cost_details}"
    
    return {"title": title, "details": details}


# Create the rule using the factory function
flight_hotel_time_gap_rule = create_time_window_rule(
    rule_id="FD-FLIGHT-HOTEL-TIME-GAP",
    title="Flight-Hotel Time Conflict",
    description="Detects logical conflicts between a user's flight arrival time and hotel check-in time",
    severity="medium",
    event_types=["FlightEvent", "HotelEvent"],
    detect_fn=detect_flight_hotel_time_gap,
    format_alert_fn=format_flight_hotel_time_gap_alert,
    window_days=3  # 3-day window to capture arrival and subsequent check-in
)