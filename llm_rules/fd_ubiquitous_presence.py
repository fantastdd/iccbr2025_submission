from typing import Dict, Any, List
from datetime import datetime, timedelta
from collections import defaultdict

from expensecbr.base import TrajectoryEvent
from expensecbr.fde import create_daily_rule


def detect_ubiquitous_presence(rule, events, context):
    """
    Detect when a user appears in multiple distant cities on the same day.
    
    This rule identifies situations where a user has activities in multiple
    geographically distant cities on the same day, which would be physically
    impossible to achieve. For example, having events in Beijing, Shanghai,
    and Guangzhou all on the same day.
    """
    # Skip if no events
    if not events:
        return False
    
    # Group events by user
    user_events = defaultdict(list)
    for event in events:
        user_events[event.user_id].append(event)
    
    # Configuration parameters with defaults
    min_city_count = context.get("ubiquitous_min_city_count", 3)  # Min cities to consider suspicious
    min_city_distance_km = context.get("ubiquitous_min_distance_km", 500)  # Min distance between cities (km)
    max_travel_speed_kmh = context.get("ubiquitous_max_travel_speed_kmh", 500)  # Max travel speed (km/h) - higher than high-speed rail
    
    suspicious_patterns = []
    
    # Check each user's events
    for user_id, user_event_list in user_events.items():
        # Skip if user has too few events
        if len(user_event_list) < min_city_count:
            continue
        
        # Get the user's cities for the day
        cities = {}  # Dict mapping city to list of events
        
        for event in user_event_list:
            # Get the event's city
            city = event.location.city
            if not city:
                continue
                
            # Add event to the city
            if city not in cities:
                cities[city] = []
            cities[city].append(event)
        
        # Skip if not enough distinct cities
        if len(cities) < min_city_count:
            continue
        
        # Check distances between cities
        distant_city_pairs = []
        city_names = list(cities.keys())
        
        for i in range(len(city_names)):
            city1 = city_names[i]
            for j in range(i + 1, len(city_names)):
                city2 = city_names[j]
                
                # Get representative events for each city
                city1_event = cities[city1][0]
                city2_event = cities[city2][0]
                
                # Calculate distance between cities
                distance = rule.get_distance(city1_event.location, city2_event.location)
                if distance is None or distance < min_city_distance_km:
                    continue
                
                # Calculate minimum travel time (hours)
                min_travel_time = distance / max_travel_speed_kmh
                
                # Calculate time available between earliest event in city1 and latest event in city2
                # We'll use the most favorable scenario for the user
                city1_events_times = [e.time_window.earliest_start for e in cities[city1]] + [e.time_window.latest_end for e in cities[city1]]
                city2_events_times = [e.time_window.earliest_start for e in cities[city2]] + [e.time_window.latest_end for e in cities[city2]]
                
                city1_earliest = min(city1_events_times)
                city1_latest = max(city1_events_times)
                city2_earliest = min(city2_events_times)
                city2_latest = max(city2_events_times)
                
                # Calculate time difference (most favorable for travel)
                time_diff_1to2 = rule.time_difference(city1_earliest, city2_latest, unit="hours")
                time_diff_2to1 = rule.time_difference(city2_earliest, city1_latest, unit="hours")
                max_time_diff = max(abs(time_diff_1to2), abs(time_diff_2to1))
                
                # If physically impossible to travel between these cities in the available time
                if max_time_diff < min_travel_time:
                    distant_city_pairs.append({
                        "city1": city1,
                        "city2": city2,
                        "distance_km": distance,
                        "min_travel_time_hours": min_travel_time,
                        "available_time_hours": max_time_diff,
                        "city1_event_id": city1_event.event_id,
                        "city2_event_id": city2_event.event_id
                    })
        
        # If we found enough distant city pairs to make this suspicious
        # We need at least min_city_count-1 pairs to connect min_city_count cities
        if len(distant_city_pairs) >= min_city_count - 1:
            # Get event types for this user
            event_types = set()
            for event in user_event_list:
                event_type = event.__class__.__name__.replace("Event", "")
                event_types.add(event_type)
            
            # Use the event with the earliest time as the primary event
            primary_event = min(user_event_list, key=lambda e: e.time_window.earliest_start)
            
            # Get all cities and their events
            all_cities = {}
            for event in user_event_list:
                city = event.location.city
                if not city:
                    continue
                if city not in all_cities:
                    all_cities[city] = []
                all_cities[city].append({
                    "event_id": event.event_id,
                    "event_type": event.__class__.__name__,
                    "time": event.time_window.earliest_start
                })
            
            # Create the result
            suspicious_patterns.append({
                "primary_event_id": primary_event.event_id,
                "user_id": user_id,
                "user_name": primary_event.user_name,
                "department": primary_event.department,
                "date": primary_event.time_window.earliest_start.date().isoformat(),
                "city_count": len(cities),
                "cities": list(cities.keys()),
                "city_details": all_cities,
                "event_types": list(event_types),
                "distant_city_pairs": distant_city_pairs,
            })
    
    return suspicious_patterns if suspicious_patterns else False


def format_ubiquitous_presence_alert(rule, events, extra_data, context):
    """Format alert details for ubiquitous presence detection"""
    # Extract key information from extra_data
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    date = extra_data.get("date", "Unknown date")
    cities = extra_data.get("cities", [])
    city_count = extra_data.get("city_count", 0)
    event_types = extra_data.get("event_types", [])
    city_details = extra_data.get("city_details", {})
    distant_city_pairs = extra_data.get("distant_city_pairs", [])
    
    # Create title
    cities_str = ", ".join(cities[:3])
    if len(cities) > 3:
        cities_str += f" and {len(cities) - 3} more"
    
    title = f"Multiple Distant Cities Same Day: {cities_str}"
    
    # Create details
    details = (
        f"User {user_name} ({user_id}) from {department} has activities in {city_count} different "
        f"geographically distant cities on {date}, which is physically impossible.\n\n"
        f"Cities visited: {', '.join(cities)}\n"
        f"Activity types: {', '.join(event_types)}\n\n"
    )
    
    # Add city details
    details += "Activities by city:\n"
    for city, events_list in city_details.items():
        details += f"- {city}: {len(events_list)} events\n"
        for idx, event in enumerate(events_list[:3]):  # Show only first 3 events per city
            time_str = event["time"].strftime("%H:%M") if event["time"] else "Unknown time"
            details += f"  {idx+1}. {event['event_type']} at {time_str}\n"
        if len(events_list) > 3:
            details += f"  ... and {len(events_list) - 3} more events\n"
    
    # Add information about distances
    details += "\nKey distances between cities:\n"
    for idx, pair in enumerate(distant_city_pairs[:5]):  # Show only first 5 pairs
        details += (
            f"{idx+1}. {pair['city1']} to {pair['city2']}: {pair['distance_km']:.1f} km\n"
            f"   Min travel time: {pair['min_travel_time_hours']:.1f} hours\n"
            f"   Available time: {pair['available_time_hours']:.1f} hours\n"
        )
    
    details += (
        f"\nThis pattern suggests:\n"
        f"- Incorrect date/time information on some events\n"
        f"- Events claimed for reimbursement that were actually attended by colleagues\n"
        f"- Possible fraudulent expense claims\n"
        f"- System data entry errors"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
ubiquitous_presence_rule = create_daily_rule(
    rule_id="FD-UBIQUITOUS-PRESENCE",
    title="Same-Day Multiple Distant Cities",
    description="Detects when a user has activities in multiple distant cities on the same day, which would be physically impossible",
    severity="high",
    event_types=None,  # Apply to all event types
    detect_fn=detect_ubiquitous_presence,
    format_alert_fn=format_ubiquitous_presence_alert,
)