from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from expensecbr.base import TrajectoryEvent, TaxiEvent, FlightEvent, RailwayEvent
from expensecbr.fde import create_time_window_rule

def detect_multi_transport_same_route_time(rule, events, context):
    """
    Detect when a user claims expenses for multiple transportation modes along the same route at the same time.
    
    This rule identifies cases where a user submits reimbursement requests for different types of
    transportation (e.g., taxi, flight, train) that cover the same route during overlapping time periods,
    which is physically impossible and indicates fraudulent expense claims.
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
        # Filter for transportation events only
        transport_events = [
            e for e in user_events 
            if isinstance(e, (TaxiEvent, FlightEvent, RailwayEvent))
        ]
        
        # Skip if less than 2 transport events (need at least 2 to have a conflict)
        if len(transport_events) < 2:
            continue
        
        # Create a list of transport routes with their details
        transport_routes = []
        
        for event in transport_events:
            # Extract from/to locations and times based on event type
            from_location = None
            to_location = None
            start_time = None
            end_time = None
            transport_type = None
            
            if isinstance(event, TaxiEvent):
                from_location = getattr(event, "from_location", None)
                to_location = getattr(event, "to_location", None)
                transport_type = "Taxi"
            elif isinstance(event, FlightEvent):
                from_location = getattr(event, "departure_location", None)
                to_location = getattr(event, "arrival_location", None)
                transport_type = "Flight"
            elif isinstance(event, RailwayEvent):
                from_location = getattr(event, "from_location", None)
                to_location = getattr(event, "to_location", None)
                transport_type = "Railway"
            
            # Skip events with missing location information
            if not from_location or not to_location:
                continue
                
            # Extract city information
            from_city = from_location.city if hasattr(from_location, "city") else None
            to_city = to_location.city if hasattr(to_location, "city") else None
            
            if not from_city or not to_city:
                continue
                
            # Get start and end times (use exact times if available, otherwise use time window)
            start_time = event.time_window.exact_start_time or event.time_window.earliest_start
            end_time = event.time_window.exact_end_time or event.time_window.latest_end
            
            # Add route to the list
            transport_routes.append({
                "event_id": event.event_id,
                "event": event,
                "from_city": from_city,
                "to_city": to_city,
                "start_time": start_time,
                "end_time": end_time,
                "transport_type": transport_type
            })
        
        # Check each pair of transport routes for conflicts
        for i in range(len(transport_routes)):
            for j in range(i + 1, len(transport_routes)):
                route1 = transport_routes[i]
                route2 = transport_routes[j]
                
                # Skip if different transport types but one is taxi (taxi could be to/from airport/station)
                if (route1["transport_type"] != route2["transport_type"] and 
                        ("Taxi" in [route1["transport_type"], route2["transport_type"]])):
                    # This isn't necessarily a conflict - could be taxi to airport then flight
                    # Only check further if times suggest impossible sequence
                    # But we'll handle this in the time overlap check below
                    pass
                
                # Check if routes match (same from/to cities, regardless of order)
                routes_match = (
                    (route1["from_city"] == route2["from_city"] and route1["to_city"] == route2["to_city"]) or
                    (route1["from_city"] == route2["to_city"] and route1["to_city"] == route2["from_city"])
                )
                
                if not routes_match:
                    continue
                
                # Check if time windows overlap
                times_overlap = (
                    (route1["start_time"] <= route2["end_time"]) and
                    (route2["start_time"] <= route1["end_time"])
                )
                
                # For events involving taxis and other transport, allow reasonable transfer time
                if "Taxi" in [route1["transport_type"], route2["transport_type"]]:
                    # If taxi is followed by other transport within reasonable time, not a conflict
                    if route1["transport_type"] == "Taxi" and route2["transport_type"] != "Taxi":
                        # Check if taxi ends shortly before other transport starts (airport transfer)
                        if 0 <= (route2["start_time"] - route1["end_time"]).total_seconds() / 60 <= 90:
                            continue
                    elif route2["transport_type"] == "Taxi" and route1["transport_type"] != "Taxi":
                        # Check if taxi starts shortly after other transport ends (airport transfer)
                        if 0 <= (route2["start_time"] - route1["end_time"]).total_seconds() / 60 <= 90:
                            continue
                
                if routes_match and times_overlap:
                    # This is a conflict - user claimed multiple transports on same route at same time
                    event1 = route1["event"]
                    event2 = route2["event"]
                    
                    # Calculate the length of the overlap
                    overlap_start = max(route1["start_time"], route2["start_time"])
                    overlap_end = min(route1["end_time"], route2["end_time"])
                    overlap_minutes = (overlap_end - overlap_start).total_seconds() / 60
                    
                    fraud_instances.append({
                        "primary_event_id": event1.event_id,
                        "secondary_event_id": event2.event_id,
                        "user_id": user_id,
                        "user_name": event1.user_name,
                        "department": event1.department,
                        "from_city": route1["from_city"],
                        "to_city": route1["to_city"],
                        "transport1_type": route1["transport_type"],
                        "transport2_type": route2["transport_type"],
                        "transport1_start": route1["start_time"].strftime("%Y-%m-%d %H:%M"),
                        "transport1_end": route1["end_time"].strftime("%Y-%m-%d %H:%M"),
                        "transport2_start": route2["start_time"].strftime("%Y-%m-%d %H:%M"),
                        "transport2_end": route2["end_time"].strftime("%Y-%m-%d %H:%M"),
                        "overlap_minutes": round(overlap_minutes),
                        "transport1_amount": getattr(event1, "amount", None),
                        "transport2_amount": getattr(event2, "amount", None),
                    })
    
    return fraud_instances if fraud_instances else False


def format_multi_transport_alert(rule, events, extra_data, context):
    """Format alert details for the multi-transport same route same time rule"""
    # Extract data from the detection results
    user_id = extra_data.get("user_id")
    user_name = extra_data.get("user_name")
    department = extra_data.get("department")
    from_city = extra_data.get("from_city")
    to_city = extra_data.get("to_city")
    transport1_type = extra_data.get("transport1_type")
    transport2_type = extra_data.get("transport2_type")
    transport1_start = extra_data.get("transport1_start")
    transport1_end = extra_data.get("transport1_end")
    transport2_start = extra_data.get("transport2_start")
    transport2_end = extra_data.get("transport2_end")
    overlap_minutes = extra_data.get("overlap_minutes")
    transport1_amount = extra_data.get("transport1_amount")
    transport2_amount = extra_data.get("transport2_amount")
    
    # Find the events
    primary_event_id = extra_data.get("primary_event_id")
    secondary_event_id = extra_data.get("secondary_event_id")
    
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    secondary_event = next((e for e in events if e.event_id == secondary_event_id), None)
    
    title = f"Multiple Transport Claims: {transport1_type} and {transport2_type} on Same Route"
    
    details = (
        f"User {user_name} ({user_id}) from {department} has claimed expenses for multiple "
        f"transportation modes along the same route at overlapping times.\n\n"
        f"Route: {from_city} to {to_city}\n\n"
        f"Transport 1: {transport1_type}\n"
        f"- Time: {transport1_start} to {transport1_end}\n"
    )
    
    if transport1_amount is not None:
        details += f"- Amount: {transport1_amount} yuan\n"
    
    details += (
        f"\nTransport 2: {transport2_type}\n"
        f"- Time: {transport2_start} to {transport2_end}\n"
    )
    
    if transport2_amount is not None:
        details += f"- Amount: {transport2_amount} yuan\n"
    
    details += (
        f"\nTime overlap: {overlap_minutes} minutes\n\n"
        f"This activity is suspicious because it's physically impossible to take multiple "
        f"modes of transportation along the same route at the same time. This suggests either "
        f"duplicate expense claims or fraudulent submissions."
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
multi_transport_rule = create_time_window_rule(
    rule_id="FD-MULTI-TRANSPORT-SAME-ROUTE-TIME",
    title="Multi-Transport Same Route Same Time",
    description="Detects when a user claims expenses for multiple transportation modes along the same route at the same time",
    severity="high",
    event_types=["TaxiEvent", "FlightEvent", "RailwayEvent"],
    detect_fn=detect_multi_transport_same_route_time,
    format_alert_fn=format_multi_transport_alert,
    window_days=3  # 3-day window to capture potential overlapping transport events
)