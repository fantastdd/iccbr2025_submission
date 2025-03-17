from typing import Dict, Any, List
from expensecbr.base import (
    TrajectoryEvent,
    TransportEvent,
    TaxiEvent,
    FlightEvent,
    RailwayEvent,
    FuelEvent
)
from expensecbr.fde import create_individual_rule


def detect_transport_reverse_time(rule, events, context):
    """
    Detect transport events with logically impossible time sequences (arrival before departure).
    
    This rule examines individual transport events and identifies cases where the
    recorded end time (arrival) is earlier than the start time (departure), which
    is physically impossible and likely indicates fraudulent activity or data error.
    """
    # We expect a list with a single event due to our individual grouping strategy
    if not events or len(events) != 1:
        return False

    event = events[0]

    # Skip if not a transport event
    if not isinstance(event, TransportEvent):
        return False
    
    # Get the event's time window
    time_window = event.time_window
    
    # Check if start time is available - need exact times to make a determination
    if not time_window.exact_start_time or not time_window.exact_end_time:
        return False
    
    start_time = time_window.exact_start_time
    end_time = time_window.exact_end_time
    
    # Calculate the time difference in minutes
    time_diff_minutes = rule.time_difference(start_time, end_time, unit="minutes")
    
    # If end time is before start time (negative difference), flag as suspicious
    if time_diff_minutes < 0:
        # Get the absolute time difference for reporting
        abs_diff_minutes = abs(time_diff_minutes)
        hours = int(abs_diff_minutes // 60)
        minutes = int(abs_diff_minutes % 60)
        
        return {
            "primary_event_id": event.event_id,
            "event_type": event.__class__.__name__,
            "start_time": start_time,
            "end_time": end_time,
            "time_diff_minutes": time_diff_minutes,
            "time_diff_formatted": f"{hours}h {minutes}m",
            "from_location": getattr(event, "from_location", None),
            "to_location": getattr(event, "to_location", None),
            "amount": event.amount
        }

    return False


def format_transport_reverse_time_alert(rule, events, extra_data, context):
    """Format alert details for reverse time transport events"""
    # Get the primary event
    event = events[0]
    
    # Get information from extra_data
    event_type = extra_data.get("event_type", "Transport")
    start_time = extra_data.get("start_time")
    end_time = extra_data.get("end_time")
    time_diff_formatted = extra_data.get("time_diff_formatted", "Unknown")
    
    # Format the timestamps
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M") if start_time else "Unknown"
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M") if end_time else "Unknown"
    
    # Format locations based on event type
    from_location = extra_data.get("from_location")
    to_location = extra_data.get("to_location")
    
    from_str = "Unknown"
    to_str = "Unknown"
    
    if from_location and hasattr(from_location, "city"):
        from_str = from_location.full_address or from_location.city
        
    if to_location and hasattr(to_location, "city"):
        to_str = to_location.full_address or to_location.city
    
    # Create event type specific details
    type_details = ""
    if isinstance(event, FlightEvent):
        type_details = f"Flight Number: {event.flight_no}\nAirline: {event.airline}\nCabin Class: {event.cabin_class}"
    elif isinstance(event, RailwayEvent):
        type_details = f"Train Number: {event.train_number}\nTrain Type: {event.train_type}\nSeat Class: {event.seat_class}"
    elif isinstance(event, TaxiEvent):
        type_details = f"Is Self-Paid: {getattr(event, 'is_self_paid', False)}"
    
    # Create the alert message
    title = f"Reversed Time in {event_type}: Arrival Before Departure"
    
    details = (
        f"User {event.user_name} ({event.user_id}) from {event.department} submitted a transport expense with "
        f"logically impossible time sequence.\n\n"
        f"Event Type: {event_type}\n"
        f"Amount: {event.amount:.2f} yuan\n"
        f"Departure Time: {start_time_str}\n"
        f"Arrival Time: {end_time_str}\n"
        f"Time Discrepancy: Arrival is {time_diff_formatted} before departure\n"
        f"From: {from_str}\n"
        f"To: {to_str}\n\n"
    )
    
    if type_details:
        details += f"Additional Details:\n{type_details}\n\n"
        
    details += (
        f"This represents a physically impossible scenario and may indicate:\n"
        f"- Intentional manipulation of expense records\n"
        f"- Data entry errors\n"
        f"- System timezone configuration issues\n"
        f"- Incorrect use of date/time fields"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
transport_reverse_time_rule = create_individual_rule(
    rule_id="FD-TRANSPORT-REVERSE-TIME",
    title="Transport Reversed Time Sequence",
    description="Detects transport events with logically impossible time sequences where arrival time is before departure time",
    severity="high",
    event_types=["TaxiEvent", "FlightEvent", "RailwayEvent", "FuelEvent"],
    detect_fn=detect_transport_reverse_time,
    format_alert_fn=format_transport_reverse_time_alert,
)