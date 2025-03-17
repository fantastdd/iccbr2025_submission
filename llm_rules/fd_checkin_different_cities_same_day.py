from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta

from expensecbr.base import TrajectoryEvent, DailyCheckInEvent
from expensecbr.fde import create_daily_rule


def detect_same_day_multi_city_checkins(rule, events, context):
    """
    Detect users checking in at multiple distant cities on the same day.
    
    This rule identifies when a user checks in at multiple cities on the same day
    where the physical distance between them would make it impossible to travel
    between them under normal transportation conditions.
    
    A typical person cannot be in two distant cities like Beijing and Guangzhou
    on the same day without using air travel, which would require transportation
    records to explain the movement between cities.
    """
    # Filter for check-in events only
    checkin_events = [e for e in events if isinstance(e, DailyCheckInEvent)]
    
    # Skip if fewer than 2 check-in events (need multiple to detect pattern)
    if len(checkin_events) < 2:
        return False
    
    # Group checkins by user
    user_checkins = {}
    for event in checkin_events:
        if event.user_id not in user_checkins:
            user_checkins[event.user_id] = []
        user_checkins[event.user_id].append(event)
    
    # Default travel speed (km/h) - conservative estimate for high-speed transport
    # This is intentionally high to minimize false positives
    max_travel_speed = context.get("max_travel_speed_kmh", 200.0)  # 200 km/h
    
    # Travel threshold - minimum hours needed between distant city check-ins
    min_travel_hours = context.get("min_travel_hours", 1.0)  # 1 hour
    
    # Minimum distance for suspicion (km)
    min_suspicious_distance = context.get("min_suspicious_distance", 150.0)  # 150 km
    
    suspicious_patterns = []
    
    # Check each user's check-ins for physically impossible scenarios
    for user_id, user_events in user_checkins.items():
        # Skip if only one check-in for the user
        if len(user_events) < 2:
            continue
        
        # Group by date
        checkins_by_date = {}
        for event in user_events:
            # Get the date (without time)
            event_date = event.time_window.earliest_start.date()
            if event_date not in checkins_by_date:
                checkins_by_date[event_date] = []
            checkins_by_date[event_date].append(event)
        
        # Check each date with multiple check-ins
        for date, daily_checkins in checkins_by_date.items():
            if len(daily_checkins) < 2:
                continue
            
            suspicious_pairs = []
            
            # Compare each pair of check-ins on this date
            for i in range(len(daily_checkins)):
                for j in range(i + 1, len(daily_checkins)):
                    event1 = daily_checkins[i]
                    event2 = daily_checkins[j]
                    
                    # Skip if events are in the same city
                    if rule.is_same_city(event1.location, event2.location):
                        continue
                    
                    # Calculate distance between locations
                    distance = rule.get_distance(event1.location, event2.location)
                    if distance is None or distance < min_suspicious_distance:
                        continue
                    
                    # Calculate minimum required travel time (hours)
                    required_travel_time = distance / max_travel_speed
                    
                    # Add minimum travel overhead
                    required_travel_time += min_travel_hours
                    
                    # Check if time windows allow for travel between check-ins
                    # This is complex with uncertain times, so we use the most favorable scenario
                    time_diff_hours = abs(
                        rule.time_difference(
                            event1.time_window.latest_end,
                            event2.time_window.earliest_start,
                            unit="hours"
                        )
                    )
                    
                    # If first case is not suspicious, try the reverse order
                    if time_diff_hours >= required_travel_time:
                        time_diff_hours = abs(
                            rule.time_difference(
                                event2.time_window.latest_end,
                                event1.time_window.earliest_start,
                                unit="hours"
                            )
                        )
                    
                    # If the time difference is less than the required travel time, this is suspicious
                    if time_diff_hours < required_travel_time:
                        suspicious_pairs.append({
                            "event1_id": event1.event_id,
                            "event2_id": event2.event_id,
                            "distance_km": distance,
                            "time_diff_hours": time_diff_hours,
                            "required_travel_hours": required_travel_time,
                            "city1": event1.location.city,
                            "city2": event2.location.city,
                            "time1": event1.time_window.earliest_start,
                            "time2": event2.time_window.earliest_start
                        })
            
            # If suspicious pairs found for this date, create an alert
            if suspicious_pairs:
                # Get a reference to one of the events for user info
                ref_event = daily_checkins[0]
                
                suspicious_patterns.append({
                    "primary_event_id": ref_event.event_id,
                    "user_id": ref_event.user_id,
                    "user_name": ref_event.user_name,
                    "department": ref_event.department,
                    "date": date.strftime("%Y-%m-%d"),
                    "suspicious_pairs": suspicious_pairs,
                    "distinct_cities": list(set([
                        event.location.city for event in daily_checkins
                    ])),
                    "checkin_count": len(daily_checkins)
                })
    
    return suspicious_patterns if suspicious_patterns else False


def format_same_day_multi_city_checkins_alert(rule, events, extra_data, context):
    """Format alert details for same-day multi-city check-ins detection"""
    # Extract key information from extra_data
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    date = extra_data.get("date", "Unknown date")
    suspicious_pairs = extra_data.get("suspicious_pairs", [])
    distinct_cities = extra_data.get("distinct_cities", [])
    
    # Format for title
    cities_str = ", ".join(distinct_cities)
    title = f"Same-Day Multi-City Check-Ins: {cities_str}"
    
    # Generate detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} checked in at multiple cities "
        f"on {date} under circumstances that appear physically impossible.\n\n"
    )
    
    # Add details for each suspicious pair
    details += "Suspicious city pairs:\n"
    for idx, pair in enumerate(suspicious_pairs):
        details += (
            f"{idx+1}. {pair['city1']} → {pair['city2']}\n"
            f"   Distance: {pair['distance_km']:.1f} km\n"
            f"   Available time: {pair['time_diff_hours']:.1f} hours\n"
            f"   Required travel time: {pair['required_travel_hours']:.1f} hours\n"
        )
    
    details += (
        f"\nThis may indicate:\n"
        f"- Check-ins made on behalf of the user by someone else\n"
        f"- Incorrect location data entered in the system\n"
        f"- Missing transportation records between these cities\n"
        f"- Fraudulent activity to claim expenses in multiple locations"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
same_day_multi_city_checkins_rule = create_daily_rule(
    rule_id="FD-CHECKIN-DIFFERENT-CITIES-SAME-DAY",
    title="Same-Day Multi-City Check-Ins",
    description="Detects when a user checks in at multiple cities on the same day where the physical distance between them would make it impossible to travel between them under normal transportation conditions",
    severity="medium",
    event_types=["DailyCheckInEvent"],
    detect_fn=detect_same_day_multi_city_checkins,
    format_alert_fn=format_same_day_multi_city_checkins_alert,
)