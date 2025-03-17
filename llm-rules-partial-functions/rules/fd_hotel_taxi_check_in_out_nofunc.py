from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent, HotelEvent, TaxiEvent
from expensecbr.fde import create_time_window_rule

def detect_hotel_taxi_checkin_checkout_conflict(rule, events, context):
    """
    Detect conflicts between taxi arrival/departure times and hotel check-in/check-out dates.
    
    This rule identifies two types of conflicts:
    1. When a taxi arrives at a hotel on a different date than the hotel check-in date
    2. When a taxi leaves a hotel on a different date than the hotel check-out date
    
    Both scenarios may indicate fraudulent expense claims or data entry errors.
    """
    if not events:
        return False
    
    # Group events by user_id
    events_by_user = {}
    for event in events:
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = []
        events_by_user[event.user_id].append(event)
    
    fraud_instances = []
    
    for user_id, user_events in events_by_user.items():
        # Filter for hotel and taxi events
        hotel_events = [e for e in user_events if isinstance(e, HotelEvent)]
        taxi_events = [e for e in user_events if isinstance(e, TaxiEvent)]
        
        # Skip if user doesn't have both hotel and taxi events
        if not hotel_events or not taxi_events:
            continue
        
        # For each hotel stay, check for related taxi events
        for hotel in hotel_events:
            hotel_location = hotel.location
            hotel_city = hotel_location.city if hotel_location else None
            hotel_specific_location = getattr(hotel_location, "specific_location", "") if hotel_location else ""
            
            if not hotel_city:
                continue
                
            # Get hotel check-in and check-out times
            check_in_time = hotel.time_window.exact_start_time or hotel.time_window.earliest_start
            check_out_time = hotel.time_window.exact_end_time or hotel.time_window.latest_end
            
            check_in_date = check_in_time.date()
            check_out_date = check_out_time.date()
            
            # Time buffer for late night arrivals (consider up to 2 AM as part of previous day)
            time_buffer_hours = 2
            
            # 1. Check taxis arriving at the hotel (potential check-in conflicts)
            for taxi in taxi_events:
                # Skip taxis without proper location information
                taxi_to_location = getattr(taxi, "to_location", None)
                if not taxi_to_location or not taxi_to_location.city:
                    continue
                
                # Get taxi arrival time and date
                taxi_arrival_time = taxi.time_window.exact_end_time or taxi.time_window.latest_end
                taxi_arrival_date = taxi_arrival_time.date()
                
                # Check if taxi destination matches hotel location
                is_same_city = taxi_to_location.city == hotel_city
                
                # If specific location available, check that too for higher confidence
                has_matching_specific_location = False
                if hotel_specific_location and hasattr(taxi_to_location, "specific_location"):
                    taxi_specific_location = getattr(taxi_to_location, "specific_location", "")
                    # Check if hotel name appears in taxi destination
                    if taxi_specific_location and hotel_specific_location:
                        # Case-insensitive partial match for hotel name in taxi destination
                        has_matching_specific_location = (
                            hotel_specific_location.lower() in taxi_specific_location.lower() or
                            taxi_specific_location.lower() in hotel_specific_location.lower()
                        )
                
                # If specific location matches, it's highly likely to be the same place
                # If only city matches, still consider it but with lower confidence
                locations_match = has_matching_specific_location or is_same_city
                
                if not locations_match:
                    continue
                
                # Special handling for late night arrivals
                is_late_night_arrival = False
                if taxi_arrival_time.hour < time_buffer_hours:
                    # For arrivals between midnight and 2 AM, adjust the date to previous day
                    adjusted_arrival_date = (taxi_arrival_time - timedelta(days=1)).date()
                    is_late_night_arrival = True
                else:
                    adjusted_arrival_date = taxi_arrival_date
                
                # Check if taxi arrival date conflicts with hotel check-in date
                date_difference = abs((adjusted_arrival_date - check_in_date).days)
                
                # Flag as conflict if dates differ by more than 0 days
                if date_difference > 0:
                    fraud_instances.append({
                        "primary_event_id": hotel.event_id,
                        "taxi_event_id": taxi.event_id,
                        "user_id": user_id,
                        "user_name": hotel.user_name,
                        "department": hotel.department,
                        "conflict_type": "check_in",
                        "hotel_city": hotel_city,
                        "hotel_location": hotel_specific_location,
                        "taxi_destination": getattr(taxi_to_location, "specific_location", ""),
                        "check_in_time": check_in_time.strftime("%Y-%m-%d %H:%M"),
                        "taxi_arrival_time": taxi_arrival_time.strftime("%Y-%m-%d %H:%M"),
                        "date_difference": date_difference,
                        "is_late_night": is_late_night_arrival,
                        "location_match_confidence": "high" if has_matching_specific_location else "medium",
                        "hotel_amount": getattr(hotel, "amount", None),
                        "taxi_amount": getattr(taxi, "amount", None)
                    })
            
            # 2. Check taxis leaving the hotel (potential check-out conflicts)
            for taxi in taxi_events:
                # Skip taxis without proper location information
                taxi_from_location = getattr(taxi, "from_location", None)
                if not taxi_from_location or not taxi_from_location.city:
                    continue
                
                # Get taxi departure time and date
                taxi_departure_time = taxi.time_window.exact_start_time or taxi.time_window.earliest_start
                taxi_departure_date = taxi_departure_time.date()
                
                # Check if taxi origin matches hotel location
                is_same_city = taxi_from_location.city == hotel_city
                
                # If specific location available, check that too for higher confidence
                has_matching_specific_location = False
                if hotel_specific_location and hasattr(taxi_from_location, "specific_location"):
                    taxi_specific_location = getattr(taxi_from_location, "specific_location", "")
                    # Check if hotel name appears in taxi origin
                    if taxi_specific_location and hotel_specific_location:
                        # Case-insensitive partial match for hotel name in taxi origin
                        has_matching_specific_location = (
                            hotel_specific_location.lower() in taxi_specific_location.lower() or
                            taxi_specific_location.lower() in hotel_specific_location.lower()
                        )
                
                # If specific location matches, it's highly likely to be the same place
                # If only city matches, still consider it but with lower confidence
                locations_match = has_matching_specific_location or is_same_city
                
                if not locations_match:
                    continue
                
                # Check if taxi departure date conflicts with hotel check-out date
                date_difference = abs((taxi_departure_date - check_out_date).days)
                
                # Flag as conflict if dates differ by more than 0 days
                if date_difference > 0:
                    fraud_instances.append({
                        "primary_event_id": hotel.event_id,
                        "taxi_event_id": taxi.event_id,
                        "user_id": user_id,
                        "user_name": hotel.user_name,
                        "department": hotel.department,
                        "conflict_type": "check_out",
                        "hotel_city": hotel_city,
                        "hotel_location": hotel_specific_location,
                        "taxi_origin": getattr(taxi_from_location, "specific_location", ""),
                        "check_out_time": check_out_time.strftime("%Y-%m-%d %H:%M"),
                        "taxi_departure_time": taxi_departure_time.strftime("%Y-%m-%d %H:%M"),
                        "date_difference": date_difference,
                        "location_match_confidence": "high" if has_matching_specific_location else "medium",
                        "hotel_amount": getattr(hotel, "amount", None),
                        "taxi_amount": getattr(taxi, "amount", None)
                    })
    
    return fraud_instances if fraud_instances else False


def format_hotel_taxi_checkin_checkout_alert(rule, events, extra_data, context):
    """Format alert details for the hotel-taxi check-in/out conflict rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    conflict_type = extra_data.get("conflict_type")
    hotel_city = extra_data.get("hotel_city")
    hotel_location = extra_data.get("hotel_location", "")
    date_difference = extra_data.get("date_difference")
    location_match_confidence = extra_data.get("location_match_confidence")
    hotel_amount = extra_data.get("hotel_amount")
    taxi_amount = extra_data.get("taxi_amount")
    
    # Format different titles and details based on conflict type
    if conflict_type == "check_in":
        taxi_destination = extra_data.get("taxi_destination", "")
        check_in_time = extra_data.get("check_in_time")
        taxi_arrival_time = extra_data.get("taxi_arrival_time")
        is_late_night = extra_data.get("is_late_night", False)
        
        hotel_display = f"{hotel_location}, {hotel_city}" if hotel_location else hotel_city
        taxi_display = taxi_destination if taxi_destination else hotel_city
        
        title = f"Check-in Conflict: Taxi Arrival and Hotel Check-in Dates Differ by {date_difference} Days"
        
        details = (
            f"User {user_name} ({user_id}) from {department} has a conflict between their "
            f"taxi arrival date and hotel check-in date.\n\n"
            f"Hotel check-in at {hotel_display}: {check_in_time}\n"
            f"Taxi arrival at {taxi_display}: {taxi_arrival_time}\n"
            f"Date difference: {date_difference} days\n"
        )
        
        if is_late_night:
            details += (
                f"Note: This was a late-night arrival (between midnight and 2 AM), "
                f"which has been taken into account when calculating date differences.\n"
            )
    
    elif conflict_type == "check_out":
        taxi_origin = extra_data.get("taxi_origin", "")
        check_out_time = extra_data.get("check_out_time")
        taxi_departure_time = extra_data.get("taxi_departure_time")
        
        hotel_display = f"{hotel_location}, {hotel_city}" if hotel_location else hotel_city
        taxi_display = taxi_origin if taxi_origin else hotel_city
        
        title = f"Check-out Conflict: Taxi Departure and Hotel Check-out Dates Differ by {date_difference} Days"
        
        details = (
            f"User {user_name} ({user_id}) from {department} has a conflict between their "
            f"taxi departure date and hotel check-out date.\n\n"
            f"Hotel check-out from {hotel_display}: {check_out_time}\n"
            f"Taxi departure from {taxi_display}: {taxi_departure_time}\n"
            f"Date difference: {date_difference} days\n"
        )
    
    else:
        title = "Hotel-Taxi Date Conflict"
        details = f"User {user_name} ({user_id}) from {department} has a conflict between taxi and hotel dates."
    
    # Add confidence information
    if location_match_confidence:
        confidence_description = (
            "The match between hotel and taxi locations is based on specific location names" 
            if location_match_confidence == "high" else 
            "The match between hotel and taxi locations is based only on city names"
        )
        details += f"\nLocation match confidence: {confidence_description}\n"
    
    # Add cost information if available
    cost_details = ""
    if hotel_amount is not None:
        cost_details += f"Hotel cost: {hotel_amount} yuan\n"
    if taxi_amount is not None:
        cost_details += f"Taxi cost: {taxi_amount} yuan\n"
    
    if cost_details:
        details += f"\nExpense details:\n{cost_details}"
    
    details += (
        f"\nThis activity is suspicious because the taxi and hotel dates should match for "
        f"legitimate travel. This conflict could indicate either fraudulent expense claims, "
        f"incorrect data entry, or unauthorized modifications to travel arrangements."
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_taxi_checkin_checkout_rule = create_time_window_rule(
    rule_id="FD-HOTEL-TAXI-CHECK-IN-OUT",
    title="Hotel-Taxi Check-in/out Date Conflict",
    description="Detects conflicts between taxi arrival/departure dates and hotel check-in/check-out dates",
    severity="medium",
    event_types=["HotelEvent", "TaxiEvent"],
    detect_fn=detect_hotel_taxi_checkin_checkout_conflict,
    format_alert_fn=format_hotel_taxi_checkin_checkout_alert,
    window_days=7  # 7-day window to capture extended stays and related taxi rides
)