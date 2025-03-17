from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent, RailwayEvent, TaxiEvent
from expensecbr.fde import create_time_window_rule
from datetime import datetime, timedelta


def detect_railway_taxi_overlap(rule, events, context):
    """
    Detect when a user has overlapping railway and taxi journeys in different cities.
    
    This rule identifies physically impossible travel patterns where a single user
    claims to be both on a train and in a taxi during the same time period, with
    the taxi being in a different city than the train's route. This indicates
    either fraudulent activity or expense data entry errors.
    """
    # Filter to include only railway and taxi events
    railway_events = [e for e in events if isinstance(e, RailwayEvent)]
    taxi_events = [e for e in events if isinstance(e, TaxiEvent)]
    
    # If there are no railway or taxi events, nothing to check
    if not railway_events or not taxi_events:
        return False
    
    suspicious_patterns = []
    
    # For each railway journey, check for overlapping taxi rides in different cities
    for railway in railway_events:
        user_id = railway.user_id
        
        # Get train origin and destination cities
        train_from_city = railway.from_location.city if railway.from_location else None
        train_to_city = railway.to_location.city if railway.to_location else None
        
        # Skip if we can't determine train cities
        if not train_from_city or not train_to_city:
            continue
        
        # Get the train time window
        train_time = railway.time_window
        
        # For each taxi ride by the same user
        for taxi in taxi_events:
            # Skip if not the same user
            if taxi.user_id != user_id:
                continue
            
            # Get taxi city
            taxi_city = taxi.location.city if taxi.location else None
            
            # Skip if we can't determine taxi city
            if not taxi_city:
                continue
            
            # Check if taxi is in a different city than both train endpoints
            if taxi_city != train_from_city and taxi_city != train_to_city:
                # Check if the time windows overlap
                if rule.do_time_intervals_overlap(train_time, taxi.time_window):
                    # Calculate the overlap duration in minutes
                    overlap_minutes = rule.get_overlap_duration(
                        train_time, taxi.time_window, unit="minutes"
                    )
                    
                    # Only consider as suspicious if overlap is substantial (more than 15 minutes)
                    # This helps avoid false positives from minor scheduling overlaps
                    if overlap_minutes >= 15:
                        # Add to suspicious patterns
                        suspicious_patterns.append({
                            "primary_event_id": railway.event_id,  # Use railway as primary event
                            "secondary_event_id": taxi.event_id,
                            "user_id": user_id,
                            "user_name": railway.user_name,
                            "department": railway.department,
                            "overlap_minutes": overlap_minutes,
                            "railway": {
                                "event_id": railway.event_id,
                                "train_number": railway.train_number,
                                "train_type": railway.train_type,
                                "from_city": train_from_city,
                                "to_city": train_to_city,
                                "start_time": train_time.earliest_start,
                                "end_time": train_time.latest_end,
                                "amount": railway.amount,
                                "seat_class": railway.seat_class
                            },
                            "taxi": {
                                "event_id": taxi.event_id,
                                "city": taxi_city,
                                "from_location": taxi.from_location.specific_location if taxi.from_location else "Unknown",
                                "to_location": taxi.to_location.specific_location if taxi.to_location else "Unknown",
                                "start_time": taxi.time_window.earliest_start,
                                "end_time": taxi.time_window.latest_end,
                                "amount": taxi.amount
                            }
                        })
    
    return suspicious_patterns if suspicious_patterns else False


def format_railway_taxi_overlap_alert(rule, events, extra_data, context):
    """Format alert details for overlapping railway and taxi journeys"""
    # Get user information
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    
    # Get railway information
    railway_info = extra_data.get("railway", {})
    train_number = railway_info.get("train_number", "Unknown")
    train_type = railway_info.get("train_type", "Unknown")
    train_from = railway_info.get("from_city", "Unknown")
    train_to = railway_info.get("to_city", "Unknown")
    train_start = railway_info.get("start_time")
    train_end = railway_info.get("end_time")
    train_amount = railway_info.get("amount", 0.0)
    seat_class = railway_info.get("seat_class", "Unknown")
    
    # Get taxi information
    taxi_info = extra_data.get("taxi", {})
    taxi_city = taxi_info.get("city", "Unknown")
    taxi_from = taxi_info.get("from_location", "Unknown")
    taxi_to = taxi_info.get("to_location", "Unknown")
    taxi_start = taxi_info.get("start_time")
    taxi_end = taxi_info.get("end_time")
    taxi_amount = taxi_info.get("amount", 0.0)
    
    # Format the times
    train_start_str = train_start.strftime("%Y-%m-%d %H:%M") if train_start else "Unknown"
    train_end_str = train_end.strftime("%Y-%m-%d %H:%M") if train_end else "Unknown"
    taxi_start_str = taxi_start.strftime("%Y-%m-%d %H:%M") if taxi_start else "Unknown"
    taxi_end_str = taxi_end.strftime("%Y-%m-%d %H:%M") if taxi_end else "Unknown"
    
    # Get overlap information
    overlap_minutes = extra_data.get("overlap_minutes", 0)
    total_amount = train_amount + taxi_amount
    
    # Create a title with key information
    title = f"Simultaneous Railway and Taxi Travel: {train_number} and {taxi_city} ({total_amount:.2f} yuan)"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} has claimed expenses for both a railway journey "
        f"and a taxi ride with a time overlap of {overlap_minutes:.0f} minutes.\n\n"
        f"This is physically impossible as the taxi is in {taxi_city}, which is different from both "
        f"the train's origin ({train_from}) and destination ({train_to}).\n\n"
        f"Railway Details:\n"
        f"- Train: {train_number} ({train_type})\n"
        f"- Route: {train_from} to {train_to}\n"
        f"- Time: {train_start_str} to {train_end_str}\n"
        f"- Seat Class: {seat_class}\n"
        f"- Amount: {train_amount:.2f} yuan\n\n"
        f"Taxi Details:\n"
        f"- City: {taxi_city}\n"
        f"- Route: {taxi_from} → {taxi_to}\n"
        f"- Time: {taxi_start_str} to {taxi_end_str}\n"
        f"- Amount: {taxi_amount:.2f} yuan\n\n"
        f"Possible Explanations:\n"
        f"- Multiple people using the same employee ID\n"
        f"- One expense was for a colleague but claimed under this ID\n"
        f"- Incorrect date/time entered for one of the journeys\n"
        f"- Duplicate or erroneous expense submission\n\n"
        f"Recommended Action: Verify both travel claims with receipts and actual itinerary"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
railway_taxi_overlap_rule = create_time_window_rule(
    rule_id="FD-RAILWAY-TAXI-OVERLAP",
    title="Railway and Taxi Journeys Overlap",
    description="Detects when a user has overlapping railway and taxi journeys in different cities, which is physically impossible",
    severity="high",
    event_types=["RailwayEvent", "TaxiEvent"],
    detect_fn=detect_railway_taxi_overlap,
    format_alert_fn=format_railway_taxi_overlap_alert,
    window_days=3  # Look at events within 3 days
)