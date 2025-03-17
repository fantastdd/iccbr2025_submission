from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent, HotelEvent, FlightEvent
from expensecbr.fde import create_time_window_rule

def detect_hotel_flight_temporal_conflict(rule, events, context):
    """
    Detect temporal conflicts between hotel stays and flight itineraries.
    
    This rule identifies logical conflicts where a user claims hotel stays that are
    incompatible with their reported flight schedules. For example, when a user claims
    to be checked into a hotel in one city while simultaneously taking a flight from
    another city, or when the flight schedule makes the hotel check-in/check-out times
    physically impossible.
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
        
        # Check each hotel event against each flight event
        for hotel in hotel_events:
            hotel_city = hotel.location.city if hotel.location else None
            if not hotel_city:
                continue
                
            # Get hotel check-in and check-out times
            # Use the exact times if available, otherwise use the time window
            check_in_time = hotel.time_window.exact_start_time or hotel.time_window.earliest_start
            check_out_time = hotel.time_window.exact_end_time or hotel.time_window.latest_end
            
            for flight in flight_events:
                # Get flight departure and arrival information
                departure_city = getattr(flight, "departure_location", None)
                departure_city = departure_city.city if departure_city else None
                
                arrival_city = getattr(flight, "arrival_location", None)
                arrival_city = arrival_city.city if arrival_city else None
                
                # Skip if flight location information is incomplete
                if not departure_city or not arrival_city:
                    continue
                
                # Get flight departure and arrival times
                departure_time = flight.time_window.exact_start_time or flight.time_window.earliest_start
                arrival_time = flight.time_window.exact_end_time or flight.time_window.latest_end
                
                # Scenario 1: Flight departs from a different city during the hotel stay
                # This is a conflict if the user is checked into a hotel in one city but departing from another
                if departure_city != hotel_city and departure_time > check_in_time and departure_time < check_out_time:
                    # Calculate buffer for transit time (2 hours before departure)
                    transit_buffer_time = departure_time - timedelta(hours=2)
                    
                    # Check if there's still a conflict after accounting for the buffer
                    if transit_buffer_time > check_in_time:
                        fraud_instances.append({
                            "primary_event_id": hotel.event_id,
                            "user_id": user_id,
                            "user_name": hotel.user_name,
                            "department": hotel.department,
                            "conflict_type": "departure_during_stay",
                            "hotel_event_id": hotel.event_id,
                            "flight_event_id": flight.event_id,
                            "hotel_city": hotel_city,
                            "flight_city": departure_city,
                            "hotel_check_in": check_in_time.strftime("%Y-%m-%d %H:%M"),
                            "hotel_check_out": check_out_time.strftime("%Y-%m-%d %H:%M"),
                            "flight_time": departure_time.strftime("%Y-%m-%d %H:%M"),
                            "flight_direction": "departure",
                        })
                
                # Scenario 2: Flight arrives at a different city during the hotel stay
                # This is a conflict if the user is checked into a hotel in one city but arriving at another
                if arrival_city != hotel_city and arrival_time > check_in_time and arrival_time < check_out_time:
                    # Calculate buffer for transit time (2 hours after arrival)
                    transit_buffer_time = arrival_time + timedelta(hours=2)
                    
                    # Check if there's still a conflict after accounting for the buffer
                    if transit_buffer_time < check_out_time:
                        fraud_instances.append({
                            "primary_event_id": hotel.event_id,
                            "user_id": user_id,
                            "user_name": hotel.user_name,
                            "department": hotel.department,
                            "conflict_type": "arrival_during_stay",
                            "hotel_event_id": hotel.event_id,
                            "flight_event_id": flight.event_id,
                            "hotel_city": hotel_city,
                            "flight_city": arrival_city,
                            "hotel_check_in": check_in_time.strftime("%Y-%m-%d %H:%M"),
                            "hotel_check_out": check_out_time.strftime("%Y-%m-%d %H:%M"),
                            "flight_time": arrival_time.strftime("%Y-%m-%d %H:%M"),
                            "flight_direction": "arrival",
                        })
                
                # Scenario 3: Hotel check-in is immediately after a flight arrival in a different city
                # This is a conflict if there's not enough time to travel between cities
                if arrival_city != hotel_city:
                    # Estimate minimum travel time between cities (at least 3 hours as a baseline)
                    min_travel_hours = 3
                    
                    # Calculate the time between flight arrival and hotel check-in
                    time_difference = (check_in_time - arrival_time).total_seconds() / 3600  # in hours
                    
                    if 0 < time_difference < min_travel_hours:
                        fraud_instances.append({
                            "primary_event_id": hotel.event_id,
                            "user_id": user_id,
                            "user_name": hotel.user_name,
                            "department": hotel.department,
                            "conflict_type": "impossible_checkin_after_flight",
                            "hotel_event_id": hotel.event_id,
                            "flight_event_id": flight.event_id,
                            "hotel_city": hotel_city,
                            "flight_city": arrival_city,
                            "hotel_check_in": check_in_time.strftime("%Y-%m-%d %H:%M"),
                            "flight_arrival": arrival_time.strftime("%Y-%m-%d %H:%M"),
                            "time_difference_hours": round(time_difference, 2),
                            "min_travel_hours": min_travel_hours,
                        })
                
                # Scenario 4: Flight departure is immediately after hotel check-out in a different city
                # This is a conflict if there's not enough time to travel between cities
                if departure_city != hotel_city:
                    # Estimate minimum travel time between cities (at least 3 hours as a baseline)
                    min_travel_hours = 3
                    
                    # Calculate the time between hotel check-out and flight departure
                    time_difference = (departure_time - check_out_time).total_seconds() / 3600  # in hours
                    
                    if 0 < time_difference < min_travel_hours:
                        fraud_instances.append({
                            "primary_event_id": hotel.event_id,
                            "user_id": user_id,
                            "user_name": hotel.user_name,
                            "department": hotel.department,
                            "conflict_type": "impossible_checkout_before_flight",
                            "hotel_event_id": hotel.event_id,
                            "flight_event_id": flight.event_id,
                            "hotel_city": hotel_city,
                            "flight_city": departure_city,
                            "hotel_check_out": check_out_time.strftime("%Y-%m-%d %H:%M"),
                            "flight_departure": departure_time.strftime("%Y-%m-%d %H:%M"),
                            "time_difference_hours": round(time_difference, 2),
                            "min_travel_hours": min_travel_hours,
                        })
    
    return fraud_instances if fraud_instances else False


def format_hotel_flight_conflict_alert(rule, events, extra_data, context):
    """Format alert details for the hotel-flight temporal conflict rule"""
    # Extract common data
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    conflict_type = extra_data.get("conflict_type")
    hotel_city = extra_data.get("hotel_city")
    
    # Find the primary event
    primary_event_id = extra_data.get("primary_event_id")
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    
    # Format title based on conflict type
    if conflict_type == "departure_during_stay":
        flight_time = extra_data.get("flight_time")
        flight_city = extra_data.get("flight_city")
        title = f"Conflict: Hotel Stay in {hotel_city} During Flight Departure from {flight_city}"
        
        details = (
            f"User {user_name} ({user_id}) from {department} has a logical conflict "
            f"between hotel stay and flight departure.\n\n"
            f"The user is checked into a hotel in {hotel_city} from "
            f"{extra_data.get('hotel_check_in')} to {extra_data.get('hotel_check_out')}, "
            f"but has a flight departing from {flight_city} at {flight_time}.\n\n"
            f"This is physically impossible as the user cannot be in two different "
            f"cities at the same time. The user would need to be at the airport in "
            f"{flight_city} for departure while supposedly staying at a hotel in {hotel_city}."
        )
    
    elif conflict_type == "arrival_during_stay":
        flight_time = extra_data.get("flight_time")
        flight_city = extra_data.get("flight_city")
        title = f"Conflict: Hotel Stay in {hotel_city} During Flight Arrival at {flight_city}"
        
        details = (
            f"User {user_name} ({user_id}) from {department} has a logical conflict "
            f"between hotel stay and flight arrival.\n\n"
            f"The user is checked into a hotel in {hotel_city} from "
            f"{extra_data.get('hotel_check_in')} to {extra_data.get('hotel_check_out')}, "
            f"but has a flight arriving at {flight_city} at {flight_time}.\n\n"
            f"This is physically impossible as the user cannot be in two different "
            f"cities at the same time. The user would be arriving at the airport in "
            f"{flight_city} while supposedly staying at a hotel in {hotel_city}."
        )
    
    elif conflict_type == "impossible_checkin_after_flight":
        flight_city = extra_data.get("flight_city")
        time_diff = extra_data.get("time_difference_hours")
        min_hours = extra_data.get("min_travel_hours")
        title = f"Conflict: Impossible Hotel Check-in Timing After Flight"
        
        details = (
            f"User {user_name} ({user_id}) from {department} has an impossibly tight "
            f"schedule between flight arrival and hotel check-in.\n\n"
            f"The user arrived in {flight_city} at {extra_data.get('flight_arrival')} "
            f"but checked into a hotel in {hotel_city} at {extra_data.get('hotel_check_in')}.\n\n"
            f"This allows only {time_diff} hours to travel between the cities, which is less "
            f"than the minimum reasonable travel time of {min_hours} hours. This schedule is "
            f"physically impossible to accomplish."
        )
    
    elif conflict_type == "impossible_checkout_before_flight":
        flight_city = extra_data.get("flight_city")
        time_diff = extra_data.get("time_difference_hours")
        min_hours = extra_data.get("min_travel_hours")
        title = f"Conflict: Impossible Hotel Check-out Timing Before Flight"
        
        details = (
            f"User {user_name} ({user_id}) from {department} has an impossibly tight "
            f"schedule between hotel check-out and flight departure.\n\n"
            f"The user checked out from a hotel in {hotel_city} at {extra_data.get('hotel_check_out')} "
            f"but has a flight departing from {flight_city} at {extra_data.get('flight_departure')}.\n\n"
            f"This allows only {time_diff} hours to travel between the cities, which is less "
            f"than the minimum reasonable travel time of {min_hours} hours. This schedule is "
            f"physically impossible to accomplish."
        )
    
    else:
        title = "Hotel-Flight Temporal Conflict"
        details = f"User {user_name} ({user_id}) from {department} has a logical conflict between hotel stay and flight schedule."
    
    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_flight_conflict_rule = create_time_window_rule(
    rule_id="FD-HOTEL-FLIGHT-TEMPORAL-CONFLICT",
    title="Hotel-Flight Temporal Conflict",
    description="Detects logical conflicts between a user's hotel check-in dates and flight itinerary dates",
    severity="high",
    event_types=["HotelEvent", "FlightEvent"],
    detect_fn=detect_hotel_flight_temporal_conflict,
    format_alert_fn=format_hotel_flight_conflict_alert,
    window_days=14  # Look at a 14-day window to capture typical business trip patterns
)