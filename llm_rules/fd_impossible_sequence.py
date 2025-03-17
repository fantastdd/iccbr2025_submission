from typing import Dict, Any, List, Optional
from datetime import timedelta

from expensecbr.base import TrajectoryEvent, TransportEvent
from expensecbr.fde import create_time_window_rule


def detect_impossible_travel(rule, events, context):
    """
    Detect physically impossible travel sequences between cities.
    
    This rule examines a user's events across different cities and identifies cases where:
    1. A user has events in different cities
    2. The time between events is too short to allow for realistic travel between locations
    3. There are no corresponding transportation events (flights, trains) that would explain the travel
    
    This may indicate either falsified expense reports or shared credential usage.
    """
    if not events or len(events) < 2:
        return False
    
    # Group events by user_id
    events_by_user = {}
    for event in events:
        if not isinstance(event, TrajectoryEvent):
            continue
            
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = []
        events_by_user[event.user_id].append(event)
    
    suspicious_sequences = []
    
    # For each user, check for impossible travel sequences
    for user_id, user_events in events_by_user.items():
        # Skip if user has fewer than 2 events
        if len(user_events) < 2:
            continue
        
        # Sort events by earliest possible start time
        sorted_events = sorted(user_events, key=lambda e: e.time_window.earliest_start)
        
        # Track transportation events to explain city changes
        transport_events = [e for e in sorted_events if isinstance(e, TransportEvent)]
        
        # Check each pair of consecutive events
        for i in range(len(sorted_events) - 1):
            event1 = sorted_events[i]
            event2 = sorted_events[i + 1]
            
            # Skip if events are in the same city
            if rule.is_same_city(event1.location, event2.location):
                continue
                
            # Calculate minimum travel time needed between these cities (in hours)
            # Assuming average speed of 100 km/h for ground travel
            distance_km = rule.get_distance(event1.location, event2.location)
            min_travel_time_hours = rule.calculate_travel_time(
                event1.location, event2.location, speed_kmh=100
            )
            
            # Convert to timedelta for easier comparison
            min_travel_time = timedelta(hours=min_travel_time_hours)
            
            # Calculate the available time between events
            time_between_events = event2.time_window.earliest_start - event1.time_window.latest_end
            
            # If time between events is less than minimum travel time
            if time_between_events < min_travel_time:
                # Check if there's a transportation event that explains this travel
                has_transport_explanation = False
                
                for transport in transport_events:
                    # Check if this transport event could explain the travel between cities
                    if (transport.time_window.earliest_start >= event1.time_window.earliest_start and
                        transport.time_window.latest_end <= event2.time_window.latest_start and
                        rule.is_same_city(transport.from_location, event1.location) and
                        rule.is_same_city(transport.to_location, event2.location)):
                        has_transport_explanation = True
                        break
                
                # If no transportation event explains the travel, flag as suspicious
                if not has_transport_explanation:
                    suspicious_sequences.append({
                        "primary_event_id": event2.event_id,
                        "user_id": user_id,
                        "user_name": event2.user_name,
                        "first_event_id": event1.event_id,
                        "first_event_time": event1.time_window.latest_end,
                        "first_event_city": event1.location.city,
                        "second_event_id": event2.event_id,
                        "second_event_time": event2.time_window.earliest_start,
                        "second_event_city": event2.location.city,
                        "time_between_events_hours": time_between_events.total_seconds() / 3600,
                        "min_travel_time_hours": min_travel_time_hours,
                        "distance_km": distance_km
                    })
    
    return suspicious_sequences if suspicious_sequences else False


def format_impossible_travel_alert(rule, events, extra_data, context):
    """Format alert details for the impossible travel sequence rule"""
    # Get the relevant events
    first_event_id = extra_data.get("first_event_id")
    second_event_id = extra_data.get("second_event_id")
    
    first_event = next((e for e in events if e.event_id == first_event_id), None)
    second_event = next((e for e in events if e.event_id == second_event_id), None)
    
    if not first_event or not second_event:
        return {
            "title": "Impossible Travel Sequence Detected",
            "details": "Error retrieving event details."
        }
    
    # Format times for display
    first_time = first_event.time_window.latest_end.strftime("%Y-%m-%d %H:%M")
    second_time = second_event.time_window.earliest_start.strftime("%Y-%m-%d %H:%M")
    
    # Calculate the time difference in a readable format
    time_diff_hours = extra_data.get("time_between_events_hours", 0)
    time_diff_str = f"{time_diff_hours:.1f} hours"
    if time_diff_hours < 1:
        time_diff_str = f"{time_diff_hours * 60:.0f} minutes"
    
    # Format the alert details
    details = (
        f"User {extra_data.get('user_name')} ({extra_data.get('user_id')}) has submitted expenses "
        f"that indicate physically impossible travel.\n\n"
        f"First event in {extra_data.get('first_event_city')} ended at {first_time}.\n"
        f"Second event in {extra_data.get('second_event_city')} started at {second_time}.\n\n"
        f"Time between events: {time_diff_str}\n"
        f"Minimum travel time needed: {extra_data.get('min_travel_time_hours', 0):.1f} hours\n"
        f"Distance between cities: {extra_data.get('distance_km', 0):.1f} km\n\n"
        f"No transportation expense was found that would explain this travel between cities."
    )
    
    title = f"Impossible Travel: {extra_data.get('first_event_city')} to {extra_data.get('second_event_city')} in {time_diff_str}"
    
    return {"title": title, "details": details}


# Create the rule using the factory function
impossible_travel_rule = create_time_window_rule(
    rule_id="FD-TRAVEL-IMPOSSIBLE-SEQUENCE",
    title="Physically Impossible Travel Sequence",
    description="Detects when a user submits expenses in different cities with insufficient time to travel between them",
    severity="high",
    window_days=3,
    event_types=["TrajectoryEvent"],  # All event types that inherit from TrajectoryEvent
    detect_fn=detect_impossible_travel,
    format_alert_fn=format_impossible_travel_alert,
)