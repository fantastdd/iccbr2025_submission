from typing import Dict, Any, List
from expensecbr.base import (
    TrajectoryEvent,
    FlightEvent,
    RailwayEvent,
    TaxiEvent,
    FuelEvent
)
from expensecbr.fde import create_time_window_rule
from datetime import datetime, timedelta


def detect_multi_transport_type_overlap(rule, events, context):
    """
    Detect when a user has overlapping transportation events of different types.
    
    This rule identifies physically impossible scenarios where a user claims to be
    using multiple transportation methods (flight, train, taxi) at the same time,
    which indicates potential fraud or expense record errors.
    """
    # Filter for all transport events
    transport_events = [
        e for e in events 
        if isinstance(e, (FlightEvent, RailwayEvent, TaxiEvent, FuelEvent))
    ]
    
    # If there are fewer than 2 transport events, no overlap is possible
    if len(transport_events) < 2:
        return False
    
    # Group events by user
    events_by_user = {}
    for event in transport_events:
        if event.user_id not in events_by_user:
            events_by_user[event.user_id] = []
        events_by_user[event.user_id].append(event)
    
    suspicious_patterns = []
    
    # For each user, check for overlapping transport events of different types
    for user_id, user_events in events_by_user.items():
        # Skip if user has only one event
        if len(user_events) < 2:
            continue
        
        # Compare each pair of events for overlap
        for i in range(len(user_events)):
            for j in range(i + 1, len(user_events)):
                event1 = user_events[i]
                event2 = user_events[j]
                
                # Skip if both events are the same type
                if type(event1) == type(event2):
                    continue
                
                # Check if time windows overlap
                if rule.do_time_intervals_overlap(event1.time_window, event2.time_window):
                    # Calculate overlap duration in minutes
                    overlap_minutes = rule.get_overlap_duration(
                        event1.time_window, event2.time_window, unit="minutes"
                    )
                    
                    # Only consider as suspicious if overlap is substantial (more than 10 minutes)
                    # This helps avoid false positives from minor scheduling overlaps
                    if overlap_minutes >= 10:
                        # Create detailed information about the overlapping events
                        transport_conflict = {
                            "primary_event_id": event1.event_id,
                            "secondary_event_id": event2.event_id,
                            "user_id": user_id,
                            "user_name": event1.user_name,
                            "department": event1.department,
                            "overlap_minutes": overlap_minutes,
                            "transport1": {
                                "event_id": event1.event_id,
                                "type": event1.__class__.__name__,
                                "start_time": event1.time_window.earliest_start,
                                "end_time": event1.time_window.latest_end,
                                "amount": event1.amount,
                                "from_city": event1.from_location.city if hasattr(event1, "from_location") and event1.from_location else None,
                                "to_city": event1.to_location.city if hasattr(event1, "to_location") and event1.to_location else None,
                                # Add type-specific fields
                                "details": _get_transport_specific_details(event1)
                            },
                            "transport2": {
                                "event_id": event2.event_id,
                                "type": event2.__class__.__name__,
                                "start_time": event2.time_window.earliest_start,
                                "end_time": event2.time_window.latest_end,
                                "amount": event2.amount,
                                "from_city": event2.from_location.city if hasattr(event2, "from_location") and event2.from_location else None,
                                "to_city": event2.to_location.city if hasattr(event2, "to_location") and event2.to_location else None,
                                # Add type-specific fields
                                "details": _get_transport_specific_details(event2)
                            }
                        }
                        
                        suspicious_patterns.append(transport_conflict)
    
    return suspicious_patterns if suspicious_patterns else False


def _get_transport_specific_details(event):
    """Helper function to extract transport-specific details"""
    if isinstance(event, FlightEvent):
        return {
            "flight_no": event.flight_no,
            "airline": event.airline,
            "cabin_class": event.cabin_class
        }
    elif isinstance(event, RailwayEvent):
        return {
            "train_number": event.train_number,
            "train_type": event.train_type,
            "seat_class": event.seat_class
        }
    elif isinstance(event, TaxiEvent):
        return {
            "is_self_paid": getattr(event, "is_self_paid", False),
            "from_location": event.from_location.specific_location if event.from_location else "Unknown",
            "to_location": event.to_location.specific_location if event.to_location else "Unknown"
        }
    elif isinstance(event, FuelEvent):
        return {
            "station_name": getattr(event, "station_name", "Unknown"),
            "fuel_type": getattr(event, "fuel_type", "Unknown")
        }
    return {}


def format_multi_transport_type_overlap_alert(rule, events, extra_data, context):
    """Format alert details for overlapping transport events of different types"""
    # Get user information
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    
    # Get transport information
    transport1 = extra_data.get("transport1", {})
    transport2 = extra_data.get("transport2", {})
    
    # Extract basic info for both transports
    type1 = transport1.get("type", "Unknown")
    type2 = transport2.get("type", "Unknown")
    start1 = transport1.get("start_time")
    end1 = transport1.get("end_time")
    start2 = transport2.get("start_time")
    end2 = transport2.get("end_time")
    amount1 = transport1.get("amount", 0.0)
    amount2 = transport2.get("amount", 0.0)
    from_city1 = transport1.get("from_city", "Unknown")
    to_city1 = transport1.get("to_city", "Unknown")
    from_city2 = transport2.get("from_city", "Unknown")
    to_city2 = transport2.get("to_city", "Unknown")
    
    # Get details for each transport type
    details1 = transport1.get("details", {})
    details2 = transport2.get("details", {})
    
    # Format times
    start1_str = start1.strftime("%Y-%m-%d %H:%M") if start1 else "Unknown"
    end1_str = end1.strftime("%Y-%m-%d %H:%M") if end1 else "Unknown"
    start2_str = start2.strftime("%Y-%m-%d %H:%M") if start2 else "Unknown"
    end2_str = end2.strftime("%Y-%m-%d %H:%M") if end2 else "Unknown"
    
    # Get overlap information
    overlap_minutes = extra_data.get("overlap_minutes", 0)
    total_amount = amount1 + amount2
    
    # Format type-specific details
    type1_details = _format_transport_details(type1, details1)
    type2_details = _format_transport_details(type2, details2)
    
    # Create a descriptive title for the alert
    title = f"Multiple Transport Types Overlap: {type1} and {type2} ({total_amount:.2f} yuan)"
    
    # Create detailed description
    details = (
        f"User {user_name} ({user_id}) from {department} has claimed expenses for two different "
        f"transportation methods that overlap by {overlap_minutes:.0f} minutes.\n\n"
        f"This is physically impossible as a person cannot use two different transportation "
        f"methods simultaneously.\n\n"
        f"Transport 1 ({type1}):\n"
        f"- Time: {start1_str} to {end1_str}\n"
    )
    
    # Add route information if available
    if from_city1 and to_city1 and from_city1 != to_city1:
        details += f"- Route: {from_city1} to {to_city1}\n"
    elif from_city1:
        details += f"- Location: {from_city1}\n"
    
    # Add amount and type-specific details
    details += f"- Amount: {amount1:.2f} yuan\n"
    details += type1_details
    
    # Add details for second transport
    details += f"\nTransport 2 ({type2}):\n"
    details += f"- Time: {start2_str} to {end2_str}\n"
    
    # Add route information if available
    if from_city2 and to_city2 and from_city2 != to_city2:
        details += f"- Route: {from_city2} to {to_city2}\n"
    elif from_city2:
        details += f"- Location: {from_city2}\n"
    
    # Add amount and type-specific details
    details += f"- Amount: {amount2:.2f} yuan\n"
    details += type2_details
    
    # Add explanations and recommendations
    details += (
        f"\nPossible Explanations:\n"
        f"- One or both transportation expenses were claimed for colleagues but under this employee's ID\n"
        f"- Incorrect dates/times entered for one of the transportation events\n"
        f"- One transportation was canceled but the expense was still submitted\n"
        f"- Multiple people sharing the same employee ID\n\n"
        f"Recommended Action: Verify both transportation expenses with receipts and actual itinerary"
    )
    
    return {"title": title, "details": details}


def _format_transport_details(transport_type, details):
    """Helper function to format transport-specific details"""
    formatted = ""
    
    if transport_type == "FlightEvent":
        flight_no = details.get("flight_no", "Unknown")
        airline = details.get("airline", "Unknown")
        cabin_class = details.get("cabin_class", "Unknown")
        formatted += f"- Flight: {flight_no} ({airline})\n"
        formatted += f"- Cabin Class: {cabin_class}"
        
    elif transport_type == "RailwayEvent":
        train_number = details.get("train_number", "Unknown")
        train_type = details.get("train_type", "Unknown")
        seat_class = details.get("seat_class", "Unknown")
        formatted += f"- Train: {train_number} ({train_type})\n"
        formatted += f"- Seat Class: {seat_class}"
        
    elif transport_type == "TaxiEvent":
        from_loc = details.get("from_location", "Unknown")
        to_loc = details.get("to_location", "Unknown")
        is_self_paid = details.get("is_self_paid", False)
        formatted += f"- Route: {from_loc} to {to_loc}\n"
        formatted += f"- Self-paid: {'Yes' if is_self_paid else 'No'}"
        
    elif transport_type == "FuelEvent":
        station = details.get("station_name", "Unknown")
        fuel_type = details.get("fuel_type", "Unknown")
        formatted += f"- Station: {station}\n"
        formatted += f"- Fuel Type: {fuel_type}"
        
    return formatted


# Create the rule using the factory function
multi_transport_type_overlap_rule = create_time_window_rule(
    rule_id="FD-MULTI-TRANSPORT-TYPE-OVERLAP",
    title="Multiple Transport Types Overlap",
    description="Detects when a user has overlapping transportation events of different types (flight, train, taxi, fuel), which is physically impossible",
    severity="high",
    event_types=["FlightEvent", "RailwayEvent", "TaxiEvent", "FuelEvent"],
    detect_fn=detect_multi_transport_type_overlap,
    format_alert_fn=format_multi_transport_type_overlap_alert,
    window_days=2  # Look at events within 2 days
)