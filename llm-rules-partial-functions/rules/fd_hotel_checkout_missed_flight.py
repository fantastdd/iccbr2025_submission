from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent, HotelEvent, FlightEvent
from expensecbr.fde import create_time_window_rule

def detect_hotel_checkout_missed_flight(rule, events, context):
    """
    Detect when a user checks out of a hotel after their scheduled flight departure time.
    
    This rule identifies cases where a user has a hotel checkout time that is later than
    their scheduled flight departure on the same day, indicating either a missed flight,
    fraudulent expense claims, or data entry errors.
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
        
        # Process each hotel checkout and check for conflicts with flight departures
        for hotel in hotel_events:
            # Get hotel location and checkout time
            hotel_city = hotel.location.city if hotel.location else None
            if not hotel_city:
                continue
                
            # Get hotel checkout time (prefer exact time, fall back to latest end time)
            checkout_time = hotel.time_window.exact_end_time or hotel.time_window.latest_end
            checkout_date = checkout_time.date()
            
            # Check for flights departing on the same day as the hotel checkout
            same_day_flights = [
                f for f in flight_events 
                if (f.time_window.exact_start_time or f.time_window.earliest_start).date() == checkout_date
            ]
            
            for flight in same_day_flights:
                # Get flight departure details
                departure_location = getattr(flight, "departure_location", None)
                departure_city = departure_location.city if departure_location else None
                
                if not departure_city:
                    continue
                
                # Get flight departure time (prefer exact time, fall back to earliest start)
                departure_time = flight.time_window.exact_start_time or flight.time_window.earliest_start
                
                # Only consider flights departing from the same city as the hotel
                if departure_city != hotel_city:
                    continue
                
                # Calculate how much buffer time is needed before flight (minimum 2 hours)
                buffer_hours = 2
                latest_checkout_time = departure_time - timedelta(hours=buffer_hours)
                
                # Check if hotel checkout is after the recommended airport arrival time
                if checkout_time > latest_checkout_time:
                    # Calculate how late the checkout is compared to when they should leave for airport
                    time_diff_minutes = round((checkout_time - latest_checkout_time).total_seconds() / 60)
                    
                    fraud_instances.append({
                        "primary_event_id": hotel.event_id,
                        "flight_event_id": flight.event_id,
                        "user_id": user_id,
                        "user_name": hotel.user_name,
                        "department": hotel.department,
                        "hotel_city": hotel_city,
                        "flight_departure_city": departure_city,
                        "checkout_time": checkout_time.strftime("%Y-%m-%d %H:%M"),
                        "flight_time": departure_time.strftime("%Y-%m-%d %H:%M"),
                        "buffer_hours": buffer_hours,
                        "minutes_late": time_diff_minutes,
                        "flight_number": getattr(flight, "flight_number", "Unknown"),
                        "hotel_amount": getattr(hotel, "amount", None),
                        "flight_amount": getattr(flight, "amount", None)
                    })
    
    return fraud_instances if fraud_instances else False


def format_hotel_checkout_missed_flight_alert(rule, events, extra_data, context):
    """Format alert details for the hotel checkout missed flight rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    city = extra_data.get("hotel_city")
    checkout_time = extra_data.get("checkout_time")
    flight_time = extra_data.get("flight_time")
    buffer_hours = extra_data.get("buffer_hours")
    minutes_late = extra_data.get("minutes_late")
    flight_number = extra_data.get("flight_number")
    hotel_amount = extra_data.get("hotel_amount")
    flight_amount = extra_data.get("flight_amount")
    
    # Format times to be more readable
    checkout_time_parts = checkout_time.split(" ")
    flight_time_parts = flight_time.split(" ")
    
    checkout_date = checkout_time_parts[0]
    checkout_time_only = checkout_time_parts[1]
    flight_time_only = flight_time_parts[1]
    
    title = f"Potential Missed Flight: Hotel Checkout After Flight Departure in {city}"
    
    details = (
        f"User {user_name} ({user_id}) from {department} has a hotel checkout time that is "
        f"after their scheduled flight departure on the same day.\n\n"
        f"Date: {checkout_date}\n"
        f"Hotel checkout time: {checkout_time_only}\n"
        f"Flight departure time: {flight_time_only}\n"
        f"Flight number: {flight_number}\n"
        f"City: {city}\n"
    )
    
    if hotel_amount is not None:
        details += f"Hotel cost: {hotel_amount} yuan\n"
    
    if flight_amount is not None:
        details += f"Flight cost: {flight_amount} yuan\n"
    
    details += (
        f"\nThe hotel checkout is {minutes_late} minutes after the recommended latest "
        f"checkout time (allowing {buffer_hours} hours before flight for airport travel "
        f"and check-in procedures).\n\n"
        f"This could indicate one of the following issues:\n"
        f"1. The user missed their flight\n"
        f"2. The hotel stay was extended but the flight was not rescheduled\n"
        f"3. The flight or hotel data was incorrectly entered\n"
        f"4. Fraudulent expense claims where either the hotel or flight didn't actually occur\n\n"
        f"This should be investigated to verify whether both expenses should be reimbursed."
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_checkout_missed_flight_rule = create_time_window_rule(
    rule_id="FD-HOTEL-CHECKOUT-MISSED-FLIGHT",
    title="Hotel Checkout After Flight Departure",
    description="Detects when a user's hotel checkout date is after their scheduled flight departure time on the same day",
    severity="medium",
    event_types=["HotelEvent", "FlightEvent"],
    detect_fn=detect_hotel_checkout_missed_flight,
    format_alert_fn=format_hotel_checkout_missed_flight_alert,
    window_days=5  # A 5-day window should be sufficient to capture most business trips
)