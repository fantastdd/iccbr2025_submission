from typing import Dict, Any, List
from expensecbr.base import (
    TrajectoryEvent,
    HotelEvent,
    DailyCheckInEvent
)
from expensecbr.fde import create_daily_rule
from datetime import datetime, timedelta


def detect_hotel_stay_during_home_city_activity(rule, events, context):
    """
    Detect when a user has hotel expenses in a different city on the same day they
    have activity records in their home city.
    
    This rule identifies physically impossible patterns where a user claims to be
    staying at a hotel in one city while also having check-in activity records in
    their home city on the same date, suggesting potential fraud.
    """
    # Filter for hotel events and daily check-in events
    hotel_events = [e for e in events if isinstance(e, HotelEvent)]
    checkin_events = [e for e in events if isinstance(e, DailyCheckInEvent)]
    
    # If there are no hotel events or no check-in events, nothing to check
    if not hotel_events or not checkin_events:
        return False
    
    # Get home locations from context
    home_locations = context.get("default_home_locations", {})
    work_locations = context.get("default_work_locations", {})  # Fallback if home not available
    
    suspicious_patterns = []
    
    # Group events by user
    events_by_user = {}
    for event in (hotel_events + checkin_events):
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = {"hotels": [], "checkins": []}
        
        if isinstance(event, HotelEvent):
            events_by_user[event.user_id]["hotels"].append(event)
        else:
            events_by_user[event.user_id]["checkins"].append(event)
    
    # For each user, check for hotel stays with home city check-ins on the same day
    for user_id, user_events in events_by_user.items():
        # Get user's home city
        home_loc = home_locations.get(user_id)
        home_city = home_loc.city if home_loc else None
        
        # If home city not available, try work city as fallback
        if not home_city:
            work_loc = work_locations.get(user_id)
            home_city = work_loc.city if work_loc else None
        
        # Skip if we can't determine home city
        if not home_city:
            continue
        
        # Check each hotel stay
        for hotel_event in user_events["hotels"]:
            hotel_city = hotel_event.location.city if hotel_event.location else None
            
            # Skip if hotel is in home city (not suspicious)
            if not hotel_city or hotel_city == home_city:
                continue
            
            # Get hotel stay dates
            hotel_start_date = hotel_event.time_window.earliest_start.date()
            hotel_end_date = hotel_event.time_window.latest_end.date()
            hotel_dates = []
            
            # Create a list of all dates covered by the hotel stay
            current_date = hotel_start_date
            while current_date <= hotel_end_date:
                hotel_dates.append(current_date)
                current_date += timedelta(days=1)
            
            # Check each check-in event
            home_checkins_during_stay = []
            for checkin in user_events["checkins"]:
                checkin_city = checkin.location.city if checkin.location else None
                
                # Skip if check-in is not in home city
                if not checkin_city or not checkin_city == home_city:
                    continue
                
                # Get check-in date
                checkin_date = checkin.time_window.earliest_start.date()
                
                # Check if this check-in date falls within hotel stay
                if checkin_date in hotel_dates:
                    home_checkins_during_stay.append(checkin)
            
            # If we found home city check-ins during hotel stay, flag as suspicious
            if home_checkins_during_stay:
                suspicious_patterns.append({
                    "primary_event_id": hotel_event.event_id,
                    "user_id": user_id,
                    "user_name": hotel_event.user_name,
                    "department": hotel_event.department,
                    "home_city": home_city,
                    "hotel_city": hotel_city,
                    "hotel_name": hotel_event.hotel_name,
                    "hotel_start": hotel_event.time_window.earliest_start,
                    "hotel_end": hotel_event.time_window.latest_end,
                    "hotel_amount": hotel_event.amount,
                    "hotel_guest_name": hotel_event.guest_name,
                    "hotel_guest_type": hotel_event.guest_type,
                    "home_checkins": [
                        {
                            "event_id": checkin.event_id,
                            "time": checkin.time_window.earliest_start,
                            "location": checkin.full_address if hasattr(checkin, "full_address") and checkin.full_address else checkin.location.full_address if checkin.location and checkin.location.full_address else home_city,
                            "activity_type": checkin.activity_type if hasattr(checkin, "activity_type") else "Check-in",
                            "customer_name": checkin.customer_name if hasattr(checkin, "customer_name") else "N/A"
                        }
                        for checkin in home_checkins_during_stay
                    ]
                })
    
    return suspicious_patterns if suspicious_patterns else False


def format_hotel_stay_during_home_city_activity_alert(rule, events, extra_data, context):
    """Format alert details for hotel stay during home city activity"""
    # Get user information
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    
    # Get hotel information
    hotel_name = extra_data.get("hotel_name", "Unknown Hotel")
    hotel_city = extra_data.get("hotel_city", "Unknown City")
    home_city = extra_data.get("home_city", "Unknown City")
    hotel_start = extra_data.get("hotel_start")
    hotel_end = extra_data.get("hotel_end")
    hotel_amount = extra_data.get("hotel_amount", 0.0)
    hotel_guest_name = extra_data.get("hotel_guest_name", "Unknown")
    hotel_guest_type = extra_data.get("hotel_guest_type", "Unknown")
    
    # Get check-in information
    home_checkins = extra_data.get("home_checkins", [])
    
    # Format dates
    hotel_start_str = hotel_start.strftime("%Y-%m-%d") if hotel_start else "Unknown"
    hotel_end_str = hotel_end.strftime("%Y-%m-%d") if hotel_end else "Unknown"
    
    # Calculate hotel stay duration
    stay_duration = "Unknown"
    if hotel_start and hotel_end:
        days = (hotel_end - hotel_start).total_seconds() / (24 * 3600)
        stay_duration = f"{days:.1f} days"
    
    # Format checkin details
    checkin_details = []
    for i, checkin in enumerate(home_checkins, 1):
        checkin_time = checkin.get("time")
        checkin_time_str = checkin_time.strftime("%Y-%m-%d %H:%M") if checkin_time else "Unknown"
        checkin_location = checkin.get("location", "Unknown")
        activity_type = checkin.get("activity_type", "Check-in")
        customer_name = checkin.get("customer_name", "N/A")
        
        checkin_details.append(
            f"Activity {i}:\n"
            f"   Time: {checkin_time_str}\n"
            f"   Location: {checkin_location}\n"
            f"   Type: {activity_type}\n"
            f"   Customer: {customer_name}"
        )
    
    # Create title with key information
    title = f"Hotel Stay During Home City Activity: {hotel_city} vs {home_city}"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} has claimed expenses for a hotel stay "
        f"in {hotel_city} during the same period they had activity records in their home city of {home_city}.\n\n"
        f"This pattern is physically impossible and suggests potential fraud or expense record errors.\n\n"
        f"Hotel Stay Details:\n"
        f"- Hotel: {hotel_name} in {hotel_city}\n"
        f"- Period: {hotel_start_str} to {hotel_end_str} ({stay_duration})\n"
        f"- Guest Name: {hotel_guest_name}\n"
        f"- Guest Type: {hotel_guest_type}\n"
        f"- Amount: {hotel_amount:.2f} yuan\n\n"
        f"Home City Activities During Hotel Stay:\n"
    )
    
    if checkin_details:
        details += "\n".join(checkin_details)
    else:
        details += "No detailed activity information available."
    
    details += "\n\n"
    details += (
        f"Possible Explanations:\n"
        f"- Hotel booking was made for someone else but claimed under this employee's ID\n"
        f"- Incorrect dates entered for either the hotel stay or home city activities\n"
        f"- System timezone configuration issues resulting in date discrepancies\n"
        f"- Multiple employees using the same ID for expense claims\n\n"
        f"Recommended Action: Verify both the hotel stay and home city activities with receipts and actual itinerary"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_stay_during_home_city_activity_rule = create_daily_rule(
    rule_id="FD-HOTEL-STAY-DURING-HOME-CITY-ACTIVITY",
    title="Hotel Stay During Home City Activity",
    description="Detects when a user has hotel expenses in a different city on the same day they have activity records in their home city",
    severity="high",
    event_types=["HotelEvent", "DailyCheckInEvent"],
    detect_fn=detect_hotel_stay_during_home_city_activity,
    format_alert_fn=format_hotel_stay_during_home_city_activity_alert,
)