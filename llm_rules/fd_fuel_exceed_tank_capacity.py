from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent, FuelEvent
from expensecbr.fde import create_individual_rule


def detect_fuel_exceed_tank_capacity(rule, events, context):
    """
    Detect fuel purchases that exceed normal vehicle tank capacity.
    
    This rule identifies physically suspicious fuel purchases where the amount of fuel
    purchased exceeds what a typical vehicle tank can hold (100 liters), suggesting
    potential fraud such as filling multiple vehicles on one receipt.
    """
    # We expect a list with a single event due to our individual grouping strategy
    if not events or len(events) != 1:
        return False

    event = events[0]

    # Skip if not a fuel event
    if not isinstance(event, FuelEvent):
        return False
    
    # Get tank capacity threshold from context or use default (100 liters)
    threshold = context.get("standard_fuel_tank_capacity", 100.0)
    
    # Get the fuel amount
    # Note: This assumes the amount field contains the fuel amount in yuan.
    # We need to estimate the actual liters based on fuel price.
    # Average fuel price in China is around 7-8 yuan per liter
    fuel_price_per_liter = context.get("fuel_price_per_liter", 7.5)
    
    # Try to get fuel amount directly if available
    fuel_amount_liters = getattr(event, "fuel_amount_liters", None)
    
    # If not available, estimate from the cost
    if fuel_amount_liters is None:
        fuel_amount_liters = event.amount / fuel_price_per_liter
    
    # Check if the fuel amount exceeds the threshold
    if fuel_amount_liters > threshold:
        # Get the fuel type for reporting
        fuel_type = getattr(event, "fuel_type", "Unknown")
        
        return {
            "primary_event_id": event.event_id,
            "amount": event.amount,
            "fuel_amount_liters": fuel_amount_liters,
            "threshold": threshold,
            "excess_amount": fuel_amount_liters - threshold,
            "fuel_type": fuel_type,
            "station_name": getattr(event, "station_name", "Unknown"),
            "estimated": fuel_amount_liters is None  # Flag if we estimated the amount
        }

    return False


def format_fuel_exceed_tank_capacity_alert(rule, events, extra_data, context):
    """Format alert details for the fuel exceed tank capacity rule"""
    # We know there's only one event in the list due to our grouping strategy
    event = events[0]
    
    # Get data from extra_data
    fuel_amount_liters = extra_data.get("fuel_amount_liters", 0.0)
    threshold = extra_data.get("threshold", 100.0)
    excess_amount = extra_data.get("excess_amount", 0.0)
    fuel_type = extra_data.get("fuel_type", "Unknown")
    station_name = extra_data.get("station_name", "Unknown")
    estimated = extra_data.get("estimated", False)
    
    # Format the location
    location_str = "Unknown"
    if event.location:
        location_str = event.location.full_address if event.location.full_address else event.location.city
    
    # Format time
    time_str = event.time_window.earliest_start.strftime("%Y-%m-%d %H:%M")
    
    # Create title with key information
    title = f"Excessive Fuel Purchase: {fuel_amount_liters:.1f} liters ({event.amount:.2f} yuan)"
    
    # Create detailed description
    estimation_note = " (estimated from cost)" if estimated else ""
    
    details = (
        f"User {event.user_name} ({event.user_id}) from {event.department} purchased "
        f"{fuel_amount_liters:.1f} liters{estimation_note} of {fuel_type} fuel at {station_name} "
        f"on {time_str} costing {event.amount:.2f} yuan.\n\n"
        f"This exceeds the typical vehicle tank capacity of {threshold:.1f} liters by "
        f"{excess_amount:.1f} liters.\n\n"
        f"Location: {location_str}\n\n"
        f"This may indicate:\n"
        f"- Multiple vehicles filled on a single receipt\n"
        f"- Additional fuel containers being filled (jerry cans, etc.)\n"
        f"- Data entry error in fuel amount or cost\n"
        f"- Large vehicle with non-standard tank capacity (e.g., truck, bus)\n\n"
        f"Recommended Action: Verify the vehicle type and actual fuel amount with the employee"
    )
    
    return {"title": title, "details": details}


# Create the rule using the factory function
fuel_exceed_tank_capacity_rule = create_individual_rule(
    rule_id="FD-FUEL-EXCEED-TANK-CAPACITY",
    title="Fuel Purchase Exceeding Tank Capacity",
    description="Detects fuel purchases that exceed normal vehicle tank capacity (100 liters), suggesting potential fraud",
    severity="medium",
    event_types=["FuelEvent"],
    detect_fn=detect_fuel_exceed_tank_capacity,
    format_alert_fn=format_fuel_exceed_tank_capacity_alert,
)