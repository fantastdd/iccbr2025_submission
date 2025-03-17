from typing import Dict, Any, List, Set, Tuple
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent, TaxiEvent, HotelEvent, FlightEvent, RailwayEvent, FuelEvent, DailyCheckInEvent
from expensecbr.fde import create_daily_rule


def detect_multi_city_single_day_no_flight(rule, events, context):
    """
    Detect when a user has events in multiple distant cities within the same day without 
    corresponding flight or train records to explain the travel.
    
    This rule identifies physically impossible travel patterns where a user appears to be in
    cities that are far apart (>500km) on the same day, but has no recorded transportation
    that would make such travel possible.
    """
    # Minimum distance between cities to be considered suspicious (in km)
    min_distance = context.get("multi_city_min_distance", 500.0)
    
    # Group events by user_id
    events_by_user = {}
    for event in events:
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = []
        events_by_user[event.user_id].append(event)
    
    # Process each user's events
    suspicious_patterns = []
    for user_id, user_events in events_by_user.items():
        # Only process if user has at least 2 events
        if len(user_events) < 2:
            continue
            
        # Extract user info from first event
        user_name = user_events[0].user_name
        department = user_events[0].department
        
        # Get the date we're analyzing (events should all be from same day due to daily rule grouping)
        date = user_events[0].time_window.earliest_start.date()
        
        # Separate transportation events (flights, trains) from other events
        flight_events = [e for e in user_events if isinstance(e, FlightEvent)]
        train_events = [e for e in user_events if isinstance(e, RailwayEvent)]
        taxi_events = [e for e in user_events if isinstance(e, TaxiEvent)]
        hotel_events = [e for e in user_events if isinstance(e, HotelEvent)]
        fuel_events = [e for e in user_events if isinstance(e, FuelEvent)]
        checkin_events = [e for e in user_events if isinstance(e, DailyCheckInEvent)]
        
        # Combine all non-transportation events for city analysis
        all_location_events = taxi_events + hotel_events + fuel_events + checkin_events
        
        # Skip if no location events
        if not all_location_events:
            continue
            
        # Collect cities from all events
        cities_with_events = {}
        for event in all_location_events:
            city = event.location.city
            if city not in cities_with_events:
                cities_with_events[city] = []
            cities_with_events[city].append(event)
            
        # Skip if user was only in one city
        if len(cities_with_events) < 2:
            continue
            
        # Check distances between cities
        distant_city_pairs = []
        cities = list(cities_with_events.keys())
        
        for i in range(len(cities)):
            for j in range(i + 1, len(cities)):
                city1, city2 = cities[i], cities[j]
                
                # Calculate distance between cities using the helper function
                distance = rule.get_distance(city1, city2)
                
                # Skip if distance calculation failed or cities are close
                if distance is None or distance < min_distance:
                    continue
                    
                # These cities are far apart, so check if user has transportation between them
                has_valid_transport = False
                
                # Check for flights between these cities
                for flight in flight_events:
                    flight_from_city = flight.from_location.city
                    flight_to_city = flight.to_location.city
                    
                    # Check if flight connects these cities (in either direction)
                    if ((flight_from_city == city1 and flight_to_city == city2) or 
                        (flight_from_city == city2 and flight_to_city == city1)):
                        has_valid_transport = True
                        break
                
                # If no flights found, check for train rides
                if not has_valid_transport:
                    for train in train_events:
                        train_from_city = train.from_location.city
                        train_to_city = train.to_location.city
                        
                        # Check if train connects these cities (in either direction)
                        if ((train_from_city == city1 and train_to_city == city2) or 
                            (train_from_city == city2 and train_to_city == city1)):
                            has_valid_transport = True
                            break
                
                # If no valid transportation found, record this suspicious city pair
                if not has_valid_transport:
                    distant_city_pairs.append({
                        "city1": city1,
                        "city2": city2,
                        "distance": distance,
                        "city1_events": cities_with_events[city1],
                        "city2_events": cities_with_events[city2]
                    })
        
        # If we found suspicious city pairs, create an alert
        if distant_city_pairs:
            # Find the most distant pair for reporting
            most_distant_pair = max(distant_city_pairs, key=lambda x: x["distance"])
            
            # Determine which event to use as primary (choose earliest event from city1)
            city1_events = sorted(most_distant_pair["city1_events"], 
                                 key=lambda e: e.time_window.earliest_start)
            primary_event = city1_events[0]
            
            # For each pair, record the transition details
            transitions = []
            for pair in distant_city_pairs:
                # Sort events by time to understand transition sequence
                city1_events = sorted(pair["city1_events"], 
                                     key=lambda e: e.time_window.earliest_start)
                city2_events = sorted(pair["city2_events"], 
                                     key=lambda e: e.time_window.earliest_start)
                
                # Determine temporal relationship (which city was "first")
                if city1_events[0].time_window.earliest_start <= city2_events[0].time_window.earliest_start:
                    from_city = pair["city1"]
                    to_city = pair["city2"]
                    from_time = city1_events[-1].time_window.latest_end
                    to_time = city2_events[0].time_window.earliest_start
                    from_event = city1_events[-1]
                    to_event = city2_events[0]
                else:
                    from_city = pair["city2"]
                    to_city = pair["city1"]
                    from_time = city2_events[-1].time_window.latest_end
                    to_time = city1_events[0].time_window.earliest_start
                    from_event = city2_events[-1]
                    to_event = city1_events[0]
                
                transitions.append({
                    "from_city": from_city,
                    "to_city": to_city,
                    "distance": pair["distance"],
                    "from_time": from_time,
                    "to_time": to_time,
                    "time_between": rule.time_difference(from_time, to_time, "hours"),
                    "from_event_id": from_event.event_id,
                    "to_event_id": to_event.event_id
                })
            
            # Collect all cities for reporting
            all_cities = list(set([p["city1"] for p in distant_city_pairs] + 
                              [p["city2"] for p in distant_city_pairs]))
            
            # Record the suspicious pattern
            suspicious_patterns.append({
                "primary_event_id": primary_event.event_id,
                "user_id": user_id,
                "user_name": user_name,
                "department": department,
                "date": date.strftime("%Y-%m-%d"),
                "cities_visited": all_cities,
                "distant_city_pairs": distant_city_pairs,
                "suspicious_transitions": transitions,
                "has_flight_records": len(flight_events) > 0,
                "has_train_records": len(train_events) > 0,
                "total_events": len(user_events),
                "location_events": len(all_location_events)
            })
    
    return suspicious_patterns if suspicious_patterns else False


def format_multi_city_single_day_no_flight_alert(rule, events, extra_data, context):
    """Format alert details for the multi-city no flight detection rule"""
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    date = extra_data.get("date", "Unknown date")
    cities = extra_data.get("cities_visited", [])
    transitions = extra_data.get("suspicious_transitions", [])
    
    # Create a concise title
    cities_str = ", ".join(cities)
    title = f"Multi-City Travel Without Transportation: {cities_str}"
    
    # Create detailed message
    details = (
        f"User {user_name} ({user_id}) from {department} has events in multiple distant cities "
        f"on {date} without any recorded flights or trains that would explain how they "
        f"moved between these cities.\n\n"
        f"Suspicious city transitions:\n"
    )
    
    # Add details for each suspicious transition
    for t in transitions:
        # Format the time between events
        hours_between = t.get("time_between", 0)
        time_format = f"{hours_between:.1f} hours between events" if hours_between else "unknown time"
        
        details += (
            f"- {t['from_city']} → {t['to_city']} "
            f"({t['distance']:.1f} km apart, {time_format})\n"
        )
    
    # Add potential explanations
    details += (
        f"\nThis may indicate:\n"
        f"- Missing transportation records\n"
        f"- Expenses submitted for travel that didn't occur\n"
        f"- Multiple people using the same employee ID for expenses\n"
        f"- Data entry errors in location information"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
multi_city_single_day_no_flight_rule = create_daily_rule(
    rule_id="FD-MULTI-CITY-SINGLE-DAY-NO-FLIGHT",
    title="Multi-City Single Day No Flight",
    description="Detects when a user has events in multiple distant cities (>500km) "
                "within the same day without corresponding flight or train records",
    severity="medium",
    event_types=["TaxiEvent", "HotelEvent", "FlightEvent", "RailwayEvent", 
                "FuelEvent", "DailyCheckInEvent"],
    detect_fn=detect_multi_city_single_day_no_flight,
    format_alert_fn=format_multi_city_single_day_no_flight_alert,
)