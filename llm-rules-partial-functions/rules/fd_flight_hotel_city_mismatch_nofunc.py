from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from expensecbr.base import TrajectoryEvent, HotelEvent, FlightEvent, RailwayEvent
from expensecbr.fde import create_time_window_rule

def detect_flight_hotel_city_mismatch(rule, events, context):
    """
    Detect mismatches between flight destinations and hotel stay cities.
    
    This rule identifies cases where a user flies to one city but stays at a hotel in a different city,
    without any transportation records (like another flight, train, etc.) that would explain
    how they traveled between these cities.
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
        
        # Create a timeline of user's locations
        timeline = []
        
        # Add flight arrivals to timeline
        for flight in flight_events:
            arrival_location = getattr(flight, "arrival_location", None)
            departure_location = getattr(flight, "departure_location", None)
            
            if arrival_location and arrival_location.city:
                arrival_time = flight.time_window.exact_end_time or flight.time_window.latest_end
                timeline.append({
                    "time": arrival_time,
                    "city": arrival_location.city,
                    "type": "flight_arrival",
                    "event_id": flight.event_id,
                    "event": flight
                })
            
            if departure_location and departure_location.city:
                departure_time = flight.time_window.exact_start_time or flight.time_window.earliest_start
                timeline.append({
                    "time": departure_time,
                    "city": departure_location.city,
                    "type": "flight_departure",
                    "event_id": flight.event_id,
                    "event": flight
                })
        
        # Add hotel stays to timeline
        for hotel in hotel_events:
            if not hotel.location or not hotel.location.city:
                continue
                
            check_in_time = hotel.time_window.exact_start_time or hotel.time_window.earliest_start
            check_out_time = hotel.time_window.exact_end_time or hotel.time_window.latest_end
            
            timeline.append({
                "time": check_in_time,
                "city": hotel.location.city,
                "type": "hotel_check_in",
                "event_id": hotel.event_id,
                "event": hotel
            })
            
            timeline.append({
                "time": check_out_time,
                "city": hotel.location.city,
                "type": "hotel_check_out",
                "event_id": hotel.event_id,
                "event": hotel
            })
        
        # Add railway events to timeline
        railway_events = [e for e in user_events if isinstance(e, RailwayEvent)]
        for railway in railway_events:
            from_location = getattr(railway, "from_location", None)
            to_location = getattr(railway, "to_location", None)
            
            if from_location and from_location.city:
                departure_time = railway.time_window.exact_start_time or railway.time_window.earliest_start
                timeline.append({
                    "time": departure_time,
                    "city": from_location.city,
                    "type": "railway_departure",
                    "event_id": railway.event_id,
                    "event": railway
                })
            
            if to_location and to_location.city:
                arrival_time = railway.time_window.exact_end_time or railway.time_window.latest_end
                timeline.append({
                    "time": arrival_time,
                    "city": to_location.city,
                    "type": "railway_arrival",
                    "event_id": railway.event_id,
                    "event": railway
                })
        
        # Sort timeline by time
        timeline.sort(key=lambda x: x["time"])
        
        # Check for mismatches between flight arrivals and subsequent hotel check-ins
        for i in range(len(timeline) - 1):
            current = timeline[i]
            next_event = timeline[i + 1]
            
            # Check if current event is a flight arrival and next event is a hotel check-in
            if current["type"] == "flight_arrival" and next_event["type"] == "hotel_check_in":
                # Skip if the cities match
                if current["city"] == next_event["city"]:
                    continue
                
                # Cities don't match - check if there's a valid connection
                flight_arrival_city = current["city"]
                hotel_city = next_event["city"]
                flight_arrival_time = current["time"]
                hotel_check_in_time = next_event["time"]
                
                # Look for any transportation events between flight arrival and hotel check-in
                has_connection = False
                for j in range(i + 1, len(timeline)):
                    intermediate = timeline[j]
                    # Stop if we've reached the hotel check-in
                    if intermediate["event_id"] == next_event["event_id"]:
                        break
                        
                    # Check if there's a transport arrival at the hotel city
                    if (intermediate["type"] in ["flight_arrival", "railway_arrival"] 
                            and intermediate["city"] == hotel_city
                            and intermediate["time"] < hotel_check_in_time):
                        has_connection = True
                        break
                
                # If no connection found, report fraud
                if not has_connection:
                    flight_event = current["event"]
                    hotel_event = next_event["event"]
                    
                    # Calculate time difference between flight arrival and hotel check-in
                    time_diff_hours = (hotel_check_in_time - flight_arrival_time).total_seconds() / 3600
                    
                    fraud_instances.append({
                        "primary_event_id": hotel_event.event_id,
                        "secondary_event_id": flight_event.event_id,
                        "user_id": user_id,
                        "user_name": hotel_event.user_name,
                        "department": hotel_event.department,
                        "flight_arrival_city": flight_arrival_city,
                        "hotel_city": hotel_city,
                        "flight_arrival_time": flight_arrival_time.strftime("%Y-%m-%d %H:%M"),
                        "hotel_check_in_time": hotel_check_in_time.strftime("%Y-%m-%d %H:%M"),
                        "time_difference_hours": round(time_diff_hours, 2)
                    })
    
    return fraud_instances if fraud_instances else False


def format_flight_hotel_city_mismatch_alert(rule, events, extra_data, context):
    """Format alert details for the flight-hotel city mismatch rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    flight_city = extra_data.get("flight_arrival_city")
    hotel_city = extra_data.get("hotel_city")
    flight_time = extra_data.get("flight_arrival_time")
    hotel_time = extra_data.get("hotel_check_in_time")
    time_diff = extra_data.get("time_difference_hours")
    
    # Find the primary and secondary events
    primary_event_id = extra_data.get("primary_event_id")
    secondary_event_id = extra_data.get("secondary_event_id")
    
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    secondary_event = next((e for e in events if e.event_id == secondary_event_id), None)
    
    title = f"City Mismatch: Flight to {flight_city}, Hotel in {hotel_city}"
    
    details = (
        f"User {user_name} ({user_id}) from {department} has a mismatch between their flight "
        f"destination and hotel location without any connecting transportation.\n\n"
        f"Flight arrived in: {flight_city} at {flight_time}\n"
        f"Hotel check-in in: {hotel_city} at {hotel_time}\n"
        f"Time between events: {time_diff} hours\n\n"
        f"This activity is suspicious because there is no record of how the user traveled "
        f"from {flight_city} to {hotel_city}. The user should have a flight, train, or other "
        f"transportation record explaining this movement between cities."
    )
    
    # Add hotel cost if available
    if primary_event and hasattr(primary_event, "amount"):
        details += f"\n\nHotel cost: {primary_event.amount} yuan"
    
    # Add flight details if available
    if secondary_event:
        flight_number = getattr(secondary_event, "flight_number", "Unknown")
        details += f"\nFlight number: {flight_number}"
        
        if hasattr(secondary_event, "amount"):
            details += f"\nFlight cost: {secondary_event.amount} yuan"
    
    return {"title": title, "details": details}


# Create the rule using the factory function
flight_hotel_city_mismatch_rule = create_time_window_rule(
    rule_id="FD-FLIGHT-HOTEL-CITY-MISMATCH",
    title="Flight-Hotel City Mismatch",
    description="Detects when a user's flight destination and hotel city don't match, with no transport record explaining the difference",
    severity="high",
    event_types=["FlightEvent", "HotelEvent", "RailwayEvent"],
    detect_fn=detect_flight_hotel_city_mismatch,
    format_alert_fn=format_flight_hotel_city_mismatch_alert,
    window_days=10  # Look at a 10-day window to capture typical business trip patterns
)