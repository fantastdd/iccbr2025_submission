from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent, HotelEvent
from expensecbr.fde import create_daily_rule
from datetime import datetime, timedelta


def detect_multi_hotel_same_night(rule, events, context):
    """
    Detect when a user has recorded stays at multiple different hotels on the same night.
    
    This rule identifies physically impossible hotel booking patterns where a single user
    claims to have stayed at more than one hotel during the same night, which is a clear
    indication of either fraudulent expense submission or expense data entry errors.
    """
    # Filter to only include hotel events
    hotel_events = [e for e in events if isinstance(e, HotelEvent)]
    
    # If there are fewer than 2 hotel events, no need to check for conflicts
    if len(hotel_events) < 2:
        return False
    
    # Group hotel events by user
    hotels_by_user = {}
    for event in hotel_events:
        if event.user_id not in hotels_by_user:
            hotels_by_user[event.user_id] = []
        hotels_by_user[event.user_id].append(event)
    
    suspicious_patterns = []
    
    # For each user, check if they have overlapping hotel stays
    for user_id, user_hotels in hotels_by_user.items():
        # Skip if user has only one hotel stay
        if len(user_hotels) < 2:
            continue
        
        # Check each pair of hotel stays for overlap
        for i in range(len(user_hotels)):
            for j in range(i + 1, len(user_hotels)):
                hotel1 = user_hotels[i]
                hotel2 = user_hotels[j]
                
                # Calculate night overlap between the two hotel stays
                if rule.do_time_intervals_overlap(hotel1.time_window, hotel2.time_window):
                    # Calculate the overlap duration in hours
                    overlap_hours = rule.get_overlap_duration(
                        hotel1.time_window, hotel2.time_window, unit="hours"
                    )
                    
                    # Only consider as suspicious if overlap is substantial (more than 8 hours)
                    # This helps avoid false positives from checkout/checkin on same day
                    if overlap_hours >= 8:
                        # Check if hotels are different
                        if (hotel1.hotel_name != hotel2.hotel_name or 
                            not rule.is_same_city(hotel1.location, hotel2.location)):
                            
                            # Create a record of the suspicious pattern
                            suspicious_patterns.append({
                                "primary_event_id": hotel1.event_id,  # Use first hotel as primary
                                "user_id": user_id,
                                "user_name": hotel1.user_name,
                                "department": hotel1.department,
                                "overlap_hours": overlap_hours,
                                "hotels": [
                                    {
                                        "event_id": hotel1.event_id,
                                        "hotel_name": hotel1.hotel_name,
                                        "location": hotel1.location,
                                        "check_in": hotel1.time_window.earliest_start,
                                        "check_out": hotel1.time_window.latest_end,
                                        "amount": hotel1.amount,
                                        "guest_name": hotel1.guest_name
                                    },
                                    {
                                        "event_id": hotel2.event_id,
                                        "hotel_name": hotel2.hotel_name,
                                        "location": hotel2.location,
                                        "check_in": hotel2.time_window.earliest_start,
                                        "check_out": hotel2.time_window.latest_end,
                                        "amount": hotel2.amount,
                                        "guest_name": hotel2.guest_name
                                    }
                                ]
                            })
    
    return suspicious_patterns if suspicious_patterns else False


def format_multi_hotel_same_night_alert(rule, events, extra_data, context):
    """Format alert details for multiple hotels on the same night"""
    # Get primary event ID from extra_data
    primary_event_id = extra_data.get("primary_event_id")
    
    # Find the primary event in the events list
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    if not primary_event:
        # Fallback if primary event not found
        return {
            "title": "Multiple Hotel Stays on Same Night",
            "details": "A user has hotel stays at multiple locations on the same night."
        }
    
    # Get hotels information
    hotels = extra_data.get("hotels", [])
    if not hotels or len(hotels) < 2:
        return {
            "title": "Multiple Hotel Stays on Same Night",
            "details": "Insufficient hotel data available."
        }
    
    # Extract user information
    user_name = extra_data.get("user_name", primary_event.user_name)
    user_id = extra_data.get("user_id", primary_event.user_id)
    department = extra_data.get("department", primary_event.department)
    
    # Format hotel details
    hotel_details = []
    total_amount = 0
    
    for i, hotel in enumerate(hotels, 1):
        hotel_name = hotel.get("hotel_name", "Unknown Hotel")
        hotel_city = hotel.get("location", {}).city if hotel.get("location") else "Unknown City"
        check_in = hotel.get("check_in")
        check_out = hotel.get("check_out")
        amount = hotel.get("amount", 0)
        guest_name = hotel.get("guest_name", "Unknown")
        
        total_amount += amount
        
        # Format dates
        check_in_str = check_in.strftime("%Y-%m-%d %H:%M") if check_in else "Unknown"
        check_out_str = check_out.strftime("%Y-%m-%d %H:%M") if check_out else "Unknown"
        
        hotel_details.append(
            f"Hotel {i}: {hotel_name} in {hotel_city}\n"
            f"   Check-in: {check_in_str}\n"
            f"   Check-out: {check_out_str}\n"
            f"   Guest Name: {guest_name}\n"
            f"   Amount: {amount:.2f} yuan"
        )
    
    # Calculate total stay duration and overlap
    overlap_hours = extra_data.get("overlap_hours", 0)
    overlap_days = overlap_hours / 24
    
    # Create title with key details
    title = f"Multiple Hotel Stays on Same Night: {len(hotels)} Hotels ({total_amount:.2f} yuan)"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} has claimed expenses for "
        f"stays at {len(hotels)} different hotels with a time overlap of {overlap_hours:.1f} hours "
        f"({overlap_days:.1f} days).\n\n"
        f"This is physically impossible as a person cannot stay at multiple hotels simultaneously.\n\n"
        f"Hotel Details:\n"
    )
    
    # Add each hotel's details
    details += "\n\n".join(hotel_details)
    
    # Add possible explanations and recommended actions
    details += "\n\n"
    details += (
        f"Possible Explanations:\n"
        f"- Multiple different people using the same employee ID\n"
        f"- Booking hotels for others but claiming under own ID\n"
        f"- Data entry errors in dates\n"
        f"- Duplicate submissions of the same hotel stay\n\n"
        f"Recommended Action: Verify actual stays with receipts and interview user"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
multi_hotel_same_night_rule = create_daily_rule(
    rule_id="FD-MULTI-HOTEL-SAME-NIGHT",
    title="Multiple Hotels Same Night",
    description="Detects when a user has recorded stays at multiple different hotels on the same night, which is physically impossible",
    severity="high",
    event_types=["HotelEvent"],
    detect_fn=detect_multi_hotel_same_night,
    format_alert_fn=format_multi_hotel_same_night_alert,
)