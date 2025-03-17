from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from expensecbr.base import TrajectoryEvent, TaxiEvent, FlightEvent, RailwayEvent
from expensecbr.fde import create_daily_rule


def detect_taxi_multicity_no_intercity_transport(rule, events, context):
    """
    Detect when a user takes taxi rides in different cities without any intercity
    transportation (flights or trains) that would explain how they moved between cities.
    
    This rule examines taxi events across cities and checks if there's a valid
    transportation method (flight or train) to explain the city changes.
    """
    # Filter for taxi events
    taxi_events = [e for e in events if isinstance(e, TaxiEvent)]
    
    # If less than 2 taxi events, no need to check
    if len(taxi_events) < 2:
        return False
    
    # Get intercity transport events (flights and railways)
    intercity_events = [e for e in events if isinstance(e, (FlightEvent, RailwayEvent))]
    
    # Group taxi events by city
    taxi_by_city = {}
    for event in taxi_events:
        city = event.from_location.city
        if city not in taxi_by_city:
            taxi_by_city[city] = []
        taxi_by_city[city].append(event)
    
    # If only one city, no issue
    if len(taxi_by_city) < 2:
        return False
    
    # Sort taxi events chronologically within each city
    for city, city_events in taxi_by_city.items():
        taxi_by_city[city] = sorted(city_events, key=lambda e: e.time_window.earliest_start)
    
    # Create a chronological sequence of city transitions
    city_transitions = []
    sorted_all_taxis = sorted(taxi_events, key=lambda e: e.time_window.earliest_start)
    
    current_city = sorted_all_taxis[0].from_location.city
    for i in range(1, len(sorted_all_taxis)):
        next_city = sorted_all_taxis[i].from_location.city
        
        # If city changed, record the transition
        if next_city != current_city:
            transition = {
                "from_city": current_city,
                "to_city": next_city,
                "from_time": sorted_all_taxis[i-1].time_window.latest_end,
                "to_time": sorted_all_taxis[i].time_window.earliest_start,
                "from_event_id": sorted_all_taxis[i-1].event_id,
                "to_event_id": sorted_all_taxis[i].event_id
            }
            city_transitions.append(transition)
            current_city = next_city
    
    # If no transitions between cities, no issue
    if not city_transitions:
        return False
    
    # Check each city transition for a valid intercity transport
    suspicious_transitions = []
    
    for transition in city_transitions:
        # Try to find a valid intercity transport for this transition
        valid_transport = find_valid_transport(
            rule,
            transition["from_city"],
            transition["to_city"],
            transition["from_time"],
            transition["to_time"],
            intercity_events
        )
        
        if not valid_transport:
            suspicious_transitions.append(transition)
    
    # If we found suspicious transitions, report them
    if suspicious_transitions:
        return {
            "primary_event_id": sorted_all_taxis[-1].event_id,
            "user_id": sorted_all_taxis[0].user_id,
            "user_name": sorted_all_taxis[0].user_name,
            "department": sorted_all_taxis[0].department,
            "suspicious_transitions": suspicious_transitions,
            "taxi_events_count": len(taxi_events),
            "cities_visited": list(taxi_by_city.keys())
        }
    
    return False


def find_valid_transport(rule, from_city, to_city, from_time, to_time, transport_events):
    """
    Find a valid transport event that explains moving from one city to another.
    
    Args:
        rule: The rule object with helper functions
        from_city: Origin city
        to_city: Destination city
        from_time: Departure time window
        to_time: Arrival time window
        transport_events: List of transport events (flights, trains)
        
    Returns:
        The transport event if found, None otherwise
    """
    for event in transport_events:
        # Check if this event connects the right cities
        if not (event.from_location.city == from_city and event.to_location.city == to_city):
            continue
            
        # Check if the timing works
        if event.time_window.earliest_start < to_time and event.time_window.latest_end > from_time:
            # The transport event overlaps with our transition window
            return event
    
    return None


def format_taxi_multicity_alert(rule, events, extra_data, context):
    """Format alert details for the taxi multi-city without intercity transport rule"""
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    transitions = extra_data.get("suspicious_transitions", [])
    cities = extra_data.get("cities_visited", [])
    
    # Format details of suspicious transitions
    transition_details = []
    for t in transitions:
        time_diff = rule.time_difference(t["from_time"], t["to_time"], "hours")
        transition_details.append(
            f"- {t['from_city']} → {t['to_city']} "
            f"({t['from_time'].strftime('%Y-%m-%d %H:%M')} to {t['to_time'].strftime('%Y-%m-%d %H:%M')}, "
            f"{time_diff:.1f} hours between taxi rides)"
        )
    
    transition_text = "\n".join(transition_details)
    
    # Create alert text
    title = f"Multi-City Taxi Use Without Intercity Transport: {', '.join(cities)}"
    
    details = (
        f"User {user_name} ({user_id}) from {department} took taxi rides in multiple cities "
        f"without any recorded intercity transportation (flights or trains) that would explain "
        f"how they moved between these cities.\n\n"
        f"Suspicious city transitions:\n{transition_text}\n\n"
        f"This may indicate:\n"
        f"- Missing transportation records\n"
        f"- Taxi expenses claimed by someone else while traveling\n"
        f"- Multiple people using the same employee ID for expenses"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
taxi_multicity_rule = create_daily_rule(
    rule_id="FD-TAXI-MULTICITY-NO-INTERCITY-TRANSPORT",
    title="Multi-City Taxi Use Without Intercity Transport",
    description="Detects when a user takes taxi rides in different cities without any intercity transportation record (flights or trains) to explain how they moved between cities",
    severity="medium",
    event_types=["TaxiEvent", "FlightEvent", "RailwayEvent"],
    detect_fn=detect_taxi_multicity_no_intercity_transport,
    format_alert_fn=format_taxi_multicity_alert,
)