from typing import Dict, Any, List
from datetime import datetime, timedelta

from expensecbr.base import TrajectoryEvent, HotelEvent
from expensecbr.fde import create_daily_rule


def detect_hotel_multiday_no_checkout(rule, events, context):
    """
    Detect users with overlapping hotel stays in different cities.
    
    This rule identifies situations where a user has hotel stays in multiple
    cities with overlapping time periods, which is physically impossible.
    For example, a user checking into hotels in both Beijing and Shanghai
    with overlapping dates would trigger this rule.
    """
    # Filter for hotel events only
    hotel_events = [e for e in events if isinstance(e, HotelEvent)]
    
    # Skip if fewer than 2 hotel events
    if len(hotel_events) < 2:
        return False
    
    # Group hotel events by user
    user_hotels = {}
    for event in hotel_events:
        if event.user_id not in user_hotels:
            user_hotels[event.user_id] = []
        user_hotels[event.user_id].append(event)
    
    suspicious_patterns = []
    
    # Check each user's hotel stays for overlapping periods in different cities
    for user_id, user_events in user_hotels.items():
        # Skip if user has only one hotel event
        if len(user_events) < 2:
            continue
        
        # Compare each pair of hotel stays
        for i in range(len(user_events)):
            for j in range(i + 1, len(user_events)):
                hotel1 = user_events[i]
                hotel2 = user_events[j]
                
                # Skip if hotels are in the same city
                if rule.is_same_city(hotel1.location, hotel2.location):
                    continue
                
                # Check if time periods overlap
                if hotel1.time_window.overlaps_with(hotel2.time_window):
                    # Calculate the overlap period
                    overlap_start = max(
                        hotel1.time_window.earliest_start, 
                        hotel2.time_window.earliest_start
                    )
                    overlap_end = min(
                        hotel1.time_window.latest_end,
                        hotel2.time_window.latest_end
                    )
                    
                    # Calculate distance between hotels
                    distance = rule.get_distance(hotel1.location, hotel2.location)
                    
                    # Add to suspicious patterns
                    suspicious_patterns.append({
                        "primary_event_id": hotel1.event_id,
                        "related_event_id": hotel2.event_id,
                        "user_id": user_id,
                        "user_name": hotel1.user_name,
                        "department": hotel1.department,
                        "hotel1_id": hotel1.event_id,
                        "hotel1_name": hotel1.hotel_name,
                        "hotel1_city": hotel1.location.city,
                        "hotel1_checkin": hotel1.time_window.earliest_start,
                        "hotel1_checkout": hotel1.time_window.latest_end,
                        "hotel2_id": hotel2.event_id,
                        "hotel2_name": hotel2.hotel_name,
                        "hotel2_city": hotel2.location.city,
                        "hotel2_checkin": hotel2.time_window.earliest_start,
                        "hotel2_checkout": hotel2.time_window.latest_end,
                        "overlap_start": overlap_start,
                        "overlap_end": overlap_end,
                        "overlap_days": (overlap_end - overlap_start).days + 1,
                        "distance_km": distance,
                        "total_cost": hotel1.amount + hotel2.amount
                    })
    
    return suspicious_patterns if suspicious_patterns else False


def format_hotel_multiday_no_checkout_alert(rule, events, extra_data, context):
    """Format alert details for overlapping hotel stays detection"""
    # Extract key information from extra_data
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    hotel1_name = extra_data.get("hotel1_name", "Unknown hotel")
    hotel1_city = extra_data.get("hotel1_city", "Unknown city")
    hotel2_name = extra_data.get("hotel2_name", "Unknown hotel") 
    hotel2_city = extra_data.get("hotel2_city", "Unknown city")
    distance_km = extra_data.get("distance_km", 0)
    overlap_days = extra_data.get("overlap_days", 0)
    
    # Format dates for display
    hotel1_checkin = extra_data.get("hotel1_checkin")
    hotel1_checkout = extra_data.get("hotel1_checkout")
    hotel2_checkin = extra_data.get("hotel2_checkin")
    hotel2_checkout = extra_data.get("hotel2_checkout")
    
    checkin1_str = hotel1_checkin.strftime("%Y-%m-%d") if hotel1_checkin else "Unknown"
    checkout1_str = hotel1_checkout.strftime("%Y-%m-%d") if hotel1_checkout else "Unknown"
    checkin2_str = hotel2_checkin.strftime("%Y-%m-%d") if hotel2_checkin else "Unknown"
    checkout2_str = hotel2_checkout.strftime("%Y-%m-%d") if hotel2_checkout else "Unknown"
    
    # Format overlap period
    overlap_start = extra_data.get("overlap_start")
    overlap_end = extra_data.get("overlap_end")
    
    overlap_start_str = overlap_start.strftime("%Y-%m-%d") if overlap_start else "Unknown"
    overlap_end_str = overlap_end.strftime("%Y-%m-%d") if overlap_end else "Unknown"
    
    # Create title
    title = f"Overlapping Hotel Stays: {hotel1_city} and {hotel2_city}"
    
    # Format detailed message
    details = (
        f"User {user_name} ({user_id}) from {department} has hotel stays in multiple "
        f"cities with overlapping dates, which is physically impossible.\n\n"
        
        f"Hotel Stay 1:\n"
        f"- Hotel: {hotel1_name} in {hotel1_city}\n"
        f"- Check-in: {checkin1_str}\n"
        f"- Check-out: {checkout1_str}\n\n"
        
        f"Hotel Stay 2:\n"
        f"- Hotel: {hotel2_name} in {hotel2_city}\n"
        f"- Check-in: {checkin2_str}\n"
        f"- Check-out: {checkout2_str}\n\n"
        
        f"Overlap Period: {overlap_start_str} to {overlap_end_str} ({overlap_days} days)\n"
        f"Distance Between Hotels: {distance_km:.1f} km\n\n"
        
        f"This pattern suggests:\n"
        f"- Hotel booking for someone else but claimed as personal stay\n"
        f"- Incorrect check-out date(s) recorded\n"
        f"- Potential duplicate or fraudulent expense claims"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_multiday_no_checkout_rule = create_daily_rule(
    rule_id="FD-HOTEL-MULTIDAY-NO-CHECKOUT",
    title="Multi-City Overlapping Hotel Stays",
    description="Detects when a user has hotel stays in different cities with overlapping time periods, which is physically impossible",
    severity="medium",
    event_types=["HotelEvent"],
    detect_fn=detect_hotel_multiday_no_checkout,
    format_alert_fn=format_hotel_multiday_no_checkout_alert,
)