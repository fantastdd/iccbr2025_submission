from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent, TaxiEvent, FlightEvent, RailwayEvent
from expensecbr.fde import create_time_window_rule

def detect_taxi_multicity_no_intercity(rule, events, context):
    """
    Detect users taking taxis in multiple cities without intercity transportation records.
    
    This rule identifies potentially suspicious activity where a user has claimed taxi expenses
    in different cities within a time window, but there are no corresponding flight or railway
    records to explain how they traveled between these cities. This could indicate either 
    fraudulent expense claims or missing transportation records.
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
        # Filter for taxi events
        taxi_events = [e for e in user_events if isinstance(e, TaxiEvent)]
        
        # Skip if less than 2 taxi events (can't be in multiple cities)
        if len(taxi_events) < 2:
            continue
        
        # Sort taxi events by earliest start time
        taxi_events.sort(key=lambda e: e.time_window.earliest_start)
        
        # Extract city information from taxi events
        cities_visited = set()
        city_to_events = {}
        
        for taxi in taxi_events:
            # Get the city from location
            city = taxi.location.city if taxi.location else None
            if not city:
                continue
                
            cities_visited.add(city)
            if city not in city_to_events:
                city_to_events[city] = []
            city_to_events[city].append(taxi)
        
        # Skip if only one city found
        if len(cities_visited) < 2:
            continue
            
        # Find all intercity transport events (flights, railway)
        intercity_events = [e for e in user_events if isinstance(e, (FlightEvent, RailwayEvent))]
        
        # For each pair of cities with taxi events, check if there's a valid intercity transport
        suspicious_city_pairs = []
        
        for city1 in cities_visited:
            for city2 in cities_visited:
                if city1 >= city2:  # Skip same city and avoid duplicates
                    continue
                    
                # Get earliest and latest taxi events in each city
                city1_taxis = sorted(city_to_events[city1], key=lambda e: e.time_window.earliest_start)
                city2_taxis = sorted(city_to_events[city2], key=lambda e: e.time_window.earliest_start)
                
                earliest_city1 = city1_taxis[0].time_window.earliest_start
                latest_city1 = city1_taxis[-1].time_window.latest_end
                
                earliest_city2 = city2_taxis[0].time_window.earliest_start
                latest_city2 = city2_taxis[-1].time_window.latest_end
                
                # Check if we have intercity transport between the cities
                has_transport = False
                
                for transport in intercity_events:
                    # Extract from and to cities based on event type
                    from_city = None
                    to_city = None
                    
                    if isinstance(transport, FlightEvent):
                        from_city = getattr(transport, "departure_location", None)
                        to_city = getattr(transport, "arrival_location", None)
                    elif isinstance(transport, RailwayEvent):
                        from_city = getattr(transport, "from_location", None)
                        to_city = getattr(transport, "to_location", None)
                    
                    # Skip if location info is missing
                    if not from_city or not to_city:
                        continue
                        
                    # Check if this transport connects our two cities
                    connects_cities = (
                        (from_city.is_same_city(city1) and to_city.is_same_city(city2)) or
                        (from_city.is_same_city(city2) and to_city.is_same_city(city1))
                    )
                    
                    if connects_cities:
                        # Check if the timing makes sense
                        transport_start = transport.time_window.earliest_start
                        transport_end = transport.time_window.latest_end
                        
                        # The transport must be between the earliest taxi in one city
                        # and the earliest taxi in the other city
                        connects_timeframes = False
                        
                        # City1 then City2
                        if latest_city1 < earliest_city2 and transport_start >= latest_city1 and transport_end <= earliest_city2:
                            connects_timeframes = True
                            
                        # City2 then City1  
                        if latest_city2 < earliest_city1 and transport_start >= latest_city2 and transport_end <= earliest_city1:
                            connects_timeframes = True
                            
                        if connects_timeframes:
                            has_transport = True
                            break
                
                # If no valid transport found, add to suspicious pairs
                if not has_transport:
                    suspicious_city_pairs.append((city1, city2))
        
        # If suspicious city pairs found, report fraud for this user
        if suspicious_city_pairs:
            # Create a fraud instance for each suspicious city pair
            for city1, city2 in suspicious_city_pairs:
                # Get relevant taxi events for evidence
                city1_taxis = city_to_events[city1]
                city2_taxis = city_to_events[city2]
                
                # Use the first taxi event in each city as primary evidence
                primary_city1_event = city1_taxis[0]
                primary_city2_event = city2_taxis[0]
                
                # Use the earliest taxi as the primary event
                primary_event = primary_city1_event if primary_city1_event.time_window.earliest_start <= primary_city2_event.time_window.earliest_start else primary_city2_event
                
                fraud_instances.append({
                    "primary_event_id": primary_event.event_id,
                    "user_id": user_id,
                    "user_name": primary_event.user_name,
                    "department": primary_event.department,
                    "city1": city1,
                    "city2": city2,
                    "city1_event_ids": [e.event_id for e in city1_taxis],
                    "city2_event_ids": [e.event_id for e in city2_taxis],
                    "city1_dates": [e.time_window.earliest_start.strftime("%Y-%m-%d") for e in city1_taxis],
                    "city2_dates": [e.time_window.earliest_start.strftime("%Y-%m-%d") for e in city2_taxis],
                })
    
    return fraud_instances if fraud_instances else False


def format_taxi_multicity_no_intercity_alert(rule, events, extra_data, context):
    """Format alert details for the taxi multicity no intercity transport rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    city1 = extra_data.get("city1")
    city2 = extra_data.get("city2")
    city1_dates = extra_data.get("city1_dates", [])
    city2_dates = extra_data.get("city2_dates", [])
    
    # Find the primary event
    primary_event_id = extra_data.get("primary_event_id")
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    
    # Format dates for display
    city1_dates_str = ", ".join(city1_dates)
    city2_dates_str = ", ".join(city2_dates)
    
    title = f"Multi-City Taxi Events Without Intercity Transport: {city1} and {city2}"
    
    details = (
        f"User {user_name} ({user_id}) from {department} has taken taxis in multiple cities "
        f"but has no record of intercity transportation between these cities.\n\n"
        f"Taxi rides in {city1} on: {city1_dates_str}\n"
        f"Taxi rides in {city2} on: {city2_dates_str}\n\n"
        f"This activity is suspicious because there is no flight, train, or other intercity "
        f"transportation record explaining how the user traveled between {city1} and {city2}. "
        f"This could indicate fraudulent expense claims or missing transportation records."
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
taxi_multicity_no_intercity_rule = create_time_window_rule(
    rule_id="FD-TAXI-MULTICITY-NO-INTERCITY-TRANSPORT",
    title="Multi-City Taxi Without Intercity Transport",
    description="Detects users taking taxis in different cities without intercity transportation records",
    severity="high",
    event_types=["TaxiEvent", "FlightEvent", "RailwayEvent"],
    detect_fn=detect_taxi_multicity_no_intercity,
    format_alert_fn=format_taxi_multicity_no_intercity_alert,
    window_days=7  # Look at a 7-day window to capture weekly travel patterns
)