from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta, date
from collections import defaultdict
from expensecbr.base import TrajectoryEvent
from expensecbr.fde import create_daily_rule

# Approximate distances between major Chinese cities in kilometers
# These are sample distances - a more comprehensive map would be needed in production
CITY_DISTANCES = {
    ("北京市", "上海市"): 1318,
    ("北京市", "广州市"): 1952,
    ("北京市", "深圳市"): 1953,
    ("北京市", "成都市"): 1671,
    ("北京市", "武汉市"): 1052,
    ("上海市", "广州市"): 1213,
    ("上海市", "深圳市"): 1207,
    ("上海市", "成都市"): 1666,
    ("上海市", "武汉市"): 686,
    ("广州市", "深圳市"): 107,
    ("广州市", "成都市"): 1235,
    ("广州市", "武汉市"): 841,
    ("深圳市", "成都市"): 1308,
    ("深圳市", "武汉市"): 938,
    ("成都市", "武汉市"): 1043,
}

# Maximum reasonable travel distance in a day (without specialized transportation)
MAX_REASONABLE_DAILY_TRAVEL_KM = 1200  # Approximate distance covered by high-speed train in a day

def get_city_distance(city1: str, city2: str) -> float:
    """Get the approximate distance between two cities in kilometers."""
    # Try to get the distance from our predefined map
    if (city1, city2) in CITY_DISTANCES:
        return CITY_DISTANCES[(city1, city2)]
    if (city2, city1) in CITY_DISTANCES:
        return CITY_DISTANCES[(city2, city1)]
        
    # If we don't have this city pair, use a default large distance for unknown pairs
    # In a real system, we'd use a more comprehensive distance database or API
    return float('inf')

def detect_ubiquitous_presence(rule, events, context):
    """
    Detect users appearing in multiple distant cities on the same day.
    
    This rule identifies physically impossible travel patterns where a user has activity
    records in multiple distant cities (like Beijing, Shanghai, Guangzhou) on the same day,
    which would be impossible to accomplish physically.
    """
    if not events:
        return False
    
    # Group events by user_id
    events_by_user = defaultdict(list)
    for event in events:
        events_by_user[event.user_id].append(event)
    
    fraud_instances = []
    
    for user_id, user_events in events_by_user.items():
        # Skip if user has too few events
        if len(user_events) < 2:
            continue
        
        # Group events by date
        events_by_date = defaultdict(list)
        for event in user_events:
            # Extract the date from the event's time window
            event_date = event.time_window.earliest_start.date()
            events_by_date[event_date].append(event)
        
        # For each date, check if user appears in distant cities
        for event_date, daily_events in events_by_date.items():
            # Skip if only one event on this date
            if len(daily_events) < 2:
                continue
            
            # Extract unique cities visited on this date
            cities_visited = {}  # city -> list of events
            
            for event in daily_events:
                # Skip events without location information
                if not hasattr(event, 'location') or not event.location or not event.location.city:
                    continue
                
                city = event.location.city
                if city not in cities_visited:
                    cities_visited[city] = []
                cities_visited[city].append(event)
            
            # Skip if user didn't visit multiple cities
            if len(cities_visited) < 2:
                continue
            
            # Check all pairs of cities
            impossible_city_pairs = []
            
            cities = list(cities_visited.keys())
            for i in range(len(cities)):
                for j in range(i+1, len(cities)):
                    city1 = cities[i]
                    city2 = cities[j]
                    
                    # Calculate distance between cities
                    distance = get_city_distance(city1, city2)
                    
                    # If distance exceeds the reasonable daily travel limit
                    if distance > MAX_REASONABLE_DAILY_TRAVEL_KM:
                        # Get the events in each city
                        city1_events = cities_visited[city1]
                        city2_events = cities_visited[city2]
                        
                        # Check if the timing makes this physically impossible
                        # Sort events by time
                        city1_events.sort(key=lambda e: e.time_window.earliest_start)
                        city2_events.sort(key=lambda e: e.time_window.earliest_start)
                        
                        # Get earliest and latest times in each city
                        city1_earliest = city1_events[0].time_window.earliest_start
                        city1_latest = city1_events[-1].time_window.latest_end
                        city2_earliest = city2_events[0].time_window.earliest_start
                        city2_latest = city2_events[-1].time_window.latest_end
                        
                        # Calculate time difference between latest event in city1 and earliest in city2
                        # and vice versa
                        time_diff1 = abs((city2_earliest - city1_latest).total_seconds() / 3600)  # hours
                        time_diff2 = abs((city1_earliest - city2_latest).total_seconds() / 3600)  # hours
                        
                        # Estimate minimum travel time (assume 300 km/h for high-speed rail as best case)
                        min_travel_hours = distance / 300
                        
                        # If minimum travel time exceeds available time between events,
                        # the travel pattern is physically impossible
                        impossible = (time_diff1 < min_travel_hours and time_diff2 < min_travel_hours)
                        
                        if impossible:
                            impossible_city_pairs.append({
                                "city1": city1,
                                "city2": city2,
                                "distance": distance,
                                "min_travel_hours": min_travel_hours,
                                "city1_event_ids": [e.event_id for e in city1_events],
                                "city2_event_ids": [e.event_id for e in city2_events],
                                "city1_earliest": city1_earliest,
                                "city1_latest": city1_latest,
                                "city2_earliest": city2_earliest,
                                "city2_latest": city2_latest,
                            })
            
            # If impossible city pairs found, report fraud
            if impossible_city_pairs:
                # Create a sample event for reference
                sample_event = daily_events[0]
                
                # Create a report for this day's impossible travel
                fraud_instances.append({
                    "primary_event_id": sample_event.event_id,
                    "user_id": user_id,
                    "user_name": sample_event.user_name,
                    "department": sample_event.department,
                    "date": event_date.strftime("%Y-%m-%d"),
                    "impossible_city_pairs": impossible_city_pairs,
                    "num_cities_visited": len(cities_visited)
                })
    
    return fraud_instances if fraud_instances else False


def format_ubiquitous_presence_alert(rule, events, extra_data, context):
    """Format alert details for the ubiquitous presence rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    date = extra_data.get("date")
    impossible_city_pairs = extra_data.get("impossible_city_pairs", [])
    num_cities = extra_data.get("num_cities_visited", 0)
    
    # Find the primary event
    primary_event_id = extra_data.get("primary_event_id")
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    
    # Format title
    title = f"Impossible Travel Pattern: {num_cities} Cities in One Day"
    
    # Format details
    city_list = []
    for city_pair in impossible_city_pairs:
        city1 = city_pair.get("city1")
        city2 = city_pair.get("city2")
        distance = city_pair.get("distance")
        min_travel_hours = city_pair.get("min_travel_hours")
        
        city1_earliest = city_pair.get("city1_earliest")
        city1_latest = city_pair.get("city1_latest")
        city2_earliest = city_pair.get("city2_earliest")
        city2_latest = city_pair.get("city2_latest")
        
        # Format timestamps
        c1_early_str = city1_earliest.strftime("%H:%M") if city1_earliest else "Unknown"
        c1_late_str = city1_latest.strftime("%H:%M") if city1_latest else "Unknown"
        c2_early_str = city2_earliest.strftime("%H:%M") if city2_earliest else "Unknown"
        c2_late_str = city2_latest.strftime("%H:%M") if city2_latest else "Unknown"
        
        city_list.append(
            f"- {city1} (active {c1_early_str}-{c1_late_str}) and {city2} (active {c2_early_str}-{c2_late_str})\n"
            f"  Distance: {distance:.0f} km, Minimum travel time: {min_travel_hours:.1f} hours"
        )
    
    city_details = "\n".join(city_list)
    
    details = (
        f"User {user_name} ({user_id}) from {department} has recorded activities in "
        f"multiple distant cities on {date}, creating a physically impossible travel pattern.\n\n"
        f"Impossible city pairs:\n{city_details}\n\n"
        f"This activity is suspicious because it would be physically impossible for a person "
        f"to be present in these distant locations within the given timeframes. This suggests "
        f"either fraudulent expense claims or incorrectly recorded event data."
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
ubiquitous_presence_rule = create_daily_rule(
    rule_id="FD-UBIQUITOUS-PRESENCE-NF",
    title="Same-Day Distant Cities Presence",
    description="Detects users appearing in multiple distant cities on the same day, which is physically impossible",
    severity="high",
    event_types=["TrajectoryEvent"],  # Use all trajectory events
    detect_fn=detect_ubiquitous_presence,
    format_alert_fn=format_ubiquitous_presence_alert
)