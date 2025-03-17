from typing import Dict, Any, List
from datetime import datetime, timedelta

from expensecbr.base import TrajectoryEvent, HotelEvent
from expensecbr.fde import create_daily_rule


def detect_hotel_duplicate_date_different_cities(rule, events, context):
    """
    Detect when a user has hotel stays in different cities on the same date.
    
    This rule identifies situations where a user has hotel bookings in multiple
    cities with the same dates, which is physically impossible. For example,
    a user checking into hotels in both Beijing and Shanghai on the same day
    would trigger this rule.
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
    
    # Check each user's hotel stays for same-date bookings in different cities
    for user_id, user_events in user_hotels.items():
        # Skip if user has only one hotel event
        if len(user_events) < 2:
            continue
        
        # Group stays by date
        stays_by_date = {}
        for event in user_events:
            # Extract date range from time window
            check_in_date = event.time_window.earliest_start.date()
            check_out_date = event.time_window.latest_end.date()
            
            # Add this stay to each date in its range
            current_date = check_in_date
            while current_date <= check_out_date:
                date_str = current_date.isoformat()
                if date_str not in stays_by_date:
                    stays_by_date[date_str] = []
                stays_by_date[date_str].append(event)
                current_date += timedelta(days=1)
        
        # Check each date for multiple cities
        for date_str, date_events in stays_by_date.items():
            if len(date_events) < 2:
                continue
            
            # Find events in different cities on this date
            cities = set()
            for event in date_events:
                cities.add(event.location.city)
            
            # If multiple cities found on same date, this is suspicious
            if len(cities) > 1:
                # Calculate distances between hotels
                hotel_distances = []
                for i in range(len(date_events)):
                    for j in range(i + 1, len(date_events)):
                        hotel1 = date_events[i]
                        hotel2 = date_events[j]
                        
                        # Skip if hotels are in the same city
                        if rule.is_same_city(hotel1.location, hotel2.location):
                            continue
                        
                        # Calculate distance between hotels
                        distance = rule.get_distance(hotel1.location, hotel2.location)
                        hotel_distances.append({
                            "hotel1_id": hotel1.event_id,
                            "hotel1_name": hotel1.hotel_name,
                            "hotel1_city": hotel1.location.city,
                            "hotel2_id": hotel2.event_id,
                            "hotel2_name": hotel2.hotel_name, 
                            "hotel2_city": hotel2.location.city,
                            "distance_km": distance
                        })
                
                # Use the first event as the primary event for the alert
                suspicious_patterns.append({
                    "primary_event_id": date_events[0].event_id,
                    "user_id": user_id,
                    "user_name": date_events[0].user_name,
                    "department": date_events[0].department,
                    "date": date_str,
                    "cities": list(cities),
                    "hotel_count": len(date_events),
                    "hotels": [
                        {
                            "event_id": e.event_id, 
                            "hotel_name": e.hotel_name,
                            "city": e.location.city,
                            "check_in": e.time_window.earliest_start,
                            "check_out": e.time_window.latest_end,
                            "amount": e.amount
                        } for e in date_events
                    ],
                    "hotel_distances": hotel_distances,
                    "total_cost": sum(e.amount for e in date_events)
                })
    
    return suspicious_patterns if suspicious_patterns else False


def format_hotel_duplicate_date_different_cities_alert(rule, events, extra_data, context):
    """Format alert details for same-date different-city hotel stays detection"""
    # Extract key information from extra_data
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    date = extra_data.get("date", "Unknown date")
    cities = extra_data.get("cities", [])
    hotels = extra_data.get("hotels", [])
    hotel_distances = extra_data.get("hotel_distances", [])
    
    # Format for title
    cities_str = ", ".join(cities)
    title = f"Same-Day Hotel Stays in Different Cities: {cities_str}"
    
    # Generate detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} has hotel stays in multiple "
        f"cities on {date}, which is physically impossible.\n\n"
        f"Cities involved: {cities_str}\n\n"
    )
    
    # Add details for each hotel
    details += "Hotel stays on this date:\n"
    for idx, hotel in enumerate(hotels):
        check_in = hotel["check_in"].strftime("%Y-%m-%d") if hotel["check_in"] else "Unknown"
        check_out = hotel["check_out"].strftime("%Y-%m-%d") if hotel["check_out"] else "Unknown"
        
        details += (
            f"{idx+1}. {hotel['hotel_name']} in {hotel['city']}\n"
            f"   Check-in: {check_in}\n"
            f"   Check-out: {check_out}\n"
            f"   Amount: {hotel['amount']} yuan\n\n"
        )
    
    # Add distance information if available
    if hotel_distances:
        details += "Distances between hotels:\n"
        for idx, dist in enumerate(hotel_distances):
            details += (
                f"{idx+1}. {dist['hotel1_name']} ({dist['hotel1_city']}) to "
                f"{dist['hotel2_name']} ({dist['hotel2_city']}): "
                f"{dist['distance_km']:.1f} km\n"
            )
    
    details += (
        f"\nThis pattern suggests:\n"
        f"- Hotel booking for someone else but claimed as personal stay\n"
        f"- Potential duplicate or fraudulent expense claims\n"
        f"- Possible data entry errors in check-in/check-out dates"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_duplicate_date_different_cities_rule = create_daily_rule(
    rule_id="FD-HOTEL-DUPLICATE-DATE-DIFFERENT-CITIES",
    title="Same-Day Hotel Stays in Different Cities",
    description="Detects when a user has hotel stays in different cities on the same date, which is physically impossible",
    severity="medium",
    event_types=["HotelEvent"],
    detect_fn=detect_hotel_duplicate_date_different_cities,
    format_alert_fn=format_hotel_duplicate_date_different_cities_alert,
)