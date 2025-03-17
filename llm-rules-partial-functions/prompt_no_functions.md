# Prompt: Generate Fraud Detection Rules

## Task Description
You're tasked with creating a fraud detection rule to identify suspicious patterns in expense reimbursement data. Your rule should be implemented as a Python function that integrates with our rule engine framework.

## Background on Fraud Detection

In our expense reimbursement system, fraud can manifest in two primary ways:

1. **Physical Impossibility:** Events that couldn't physically occur, such as being in two distant cities simultaneously or traveling between cities faster than physically possible.

2. **Policy Violations:** Activities that violate company policies, such as claiming reimbursement for commuting between home and office or splitting a large expense into multiple smaller ones to bypass approval thresholds.

## Event Data Model

We model business activities as spatiotemporal events with the following characteristics:
- Each event has a location, time window, user information, and event-specific details
- Time windows include earliest/latest start and end times to represent certainty levels
- Location information includes city and possibly specific address details

### Event Types

Our system tracks these primary event types:

1. **TaxiEvent:** Taxi ride details (from/to locations, amount, time)
2. **HotelEvent:** Hotel stay information (location, check-in/out times, amount)
3. **FlightEvent:** Air travel details (departure/arrival locations, flight number)
4. **RailwayEvent:** Train journey information (from/to stations, train details)
5. **FuelEvent:** Fuel purchase information (station, amount, car details)
6. **DailyCheckInEvent:** Business visit check-ins (customer location, activity details)

### Core Data Structures

Here are the key data structures with their important fields:

```python
class TrajectoryEvent:
    event_id: str              # Unique identifier
    user_id: str               # User's ID
    user_name: str             # User's name
    department: str            # User's department
    amount: float              # Cost in yuan
    remark: str                # User remarks
    time_window: TimeWindow    # Time window of the event
    location: Location         # Location information

class Location:
    city: str                     # City name (always ends with "市")
    specific_location: str        # Detailed location (may be None)
    
    # Access methods
    @property
    def full_address(self) -> Optional[str]:  # Returns formatted address or None
    def is_same_city(self, other: Location) -> bool:  # Compare cities

class TimeWindow:
    earliest_start: datetime      # Earliest possible start time
    latest_start: datetime        # Latest possible start time
    earliest_end: datetime        # Earliest possible end time
    latest_end: datetime          # Latest possible end time
    
    # Access methods
    def overlaps_with(self, other: TimeWindow) -> bool:  # Check for overlap
    @property
    def exact_start_time(self) -> Optional[datetime]:  # Returns exact start time if no uncertainty
    @property
    def exact_end_time(self) -> Optional[datetime]:  # Returns exact end time if no uncertainty
```


### Context Data
```python
# Context provides configuration data and defaults
context.get("default_office_locations", {})  # Dict mapping city name to Location
context.get("default_work_locations", {})    # Dict mapping user_id to work Location
context.get("default_home_locations", {})    # Dict mapping user_id to home Location
context.get("working_hours", {"start": 9, "end": 18})  # Default work hours
```

## Rule Framework

Rules are created using one of these factory functions based on how events should be grouped:

```python
# Process events individually (one by one)
create_individual_rule(rule_id, title, description, event_types, severity, detect_fn, format_alert_fn)

# Process events grouped by date
create_daily_rule(rule_id, title, description, event_types, severity, detect_fn, format_alert_fn)

# Process events in sliding time windows
create_time_window_rule(rule_id, title, description, event_types, severity, detect_fn, format_alert_fn, window_days=3)
```

### Detection Function Structure
```python
def detect_pattern(rule, events, context):
    """
    Detect [specific fraud pattern].
    
    This rule identifies [detailed description of what the rule checks and why it's suspicious].
    """
    # Your detection logic here
    
    # For no fraud detected:
    return False
    
    # For fraud detected (single instance):
    return {
        "primary_event_id": event.event_id,  # ID of the primary suspicious event
        "key1": value1,                      # Additional evidence/details
        "key2": value2,                      # More supporting information
    }
    
    # For multiple fraud instances:
    return [
        {"primary_event_id": event1.event_id, "details": "..."},
        {"primary_event_id": event2.event_id, "details": "..."},
    ]
```

### Alert Formatting Function Structure
```python
def format_pattern_alert(rule, events, extra_data, context):
    """Format alert details for [rule name]"""
    # Get data from the detection result
    primary_event_id = extra_data.get("primary_event_id")
    
    # Find the primary event
    primary_event = next((e for e in events if e.event_id == primary_event_id), None)
    if not primary_event:
        return {"title": "Alert", "details": "Suspicious activity detected"}
    
    # Create user-friendly alert
    title = f"Specific Fraud Pattern: {key_detail}"
    
    details = (
        f"User {primary_event.user_name} ({primary_event.user_id}) from {primary_event.department} "
        f"engaged in suspicious activity: [clear explanation of what was detected].\n\n"
        f"Evidence: [specific details showing why this is suspicious]"
    )
    
    return {"title": title, "details": details}
```

## Example Rules

### Example 1: High-Value Taxi Detection
This rule detects unusually expensive individual taxi rides:

```python
from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent, TaxiEvent
from expensecbr.fde import create_individual_rule

def detect_high_value_taxi(rule, events, context):
    """
    Detect unusually expensive taxi rides that may indicate fraud.

    This rule examines individual taxi events and flags those with costs exceeding
    a threshold amount (50 yuan by default).
    """
    # We expect a list with a single event due to our individual grouping strategy
    if not events or len(events) != 1:
        return False

    event = events[0]

    # Skip if not a taxi event or is self-paid
    if not isinstance(event, TaxiEvent) or getattr(event, "is_self_paid", False):
        return False

    # Get threshold from context or use default
    threshold = context.get("taxi_high_value_threshold", 50.0)

    # Check if the amount exceeds the threshold
    if event.amount > threshold:
        return {
            "primary_event_id": event.event_id,
            "amount": event.amount,
            "threshold": threshold,
            "excess_amount": event.amount - threshold,
        }

    return False

def format_high_value_alert(rule, events, extra_data, context):
    """Format alert details for the high-value taxi rule"""
    # We know there's only one event in the list due to our grouping strategy
    event = events[0]

    # Format locations
    from_location = getattr(event, "from_location", None)
    to_location = getattr(event, "to_location", None)

    from_loc_str = (
        from_location.full_address if from_location and from_location.full_address else "Unknown"
    )
    to_loc_str = (
        to_location.full_address if to_location and to_location.full_address else "Unknown"
    )

    # Format time
    time_str = event.time_window.earliest_start.strftime("%Y-%m-%d %H:%M")

    details = (
        f"User {event.user_name} ({event.user_id}) took an expensive taxi ride on {time_str} "
        f"from {from_loc_str} to {to_loc_str} costing {event.amount} yuan. "
        f"This exceeds the threshold of {extra_data.get('threshold', 50.0)} yuan by "
        f"{extra_data.get('excess_amount', 0.0):.2f} yuan."
    )

    return {"title": f"High-Value Taxi Ride: {event.amount} yuan", "details": details}

# Create the rule using the factory function
high_value_taxi_rule = create_individual_rule(
    rule_id="FD-TAXI-HIGH-VALUE",
    title="High-Value Taxi Rides",
    description="Detects unusually expensive taxi rides that may indicate fraud",
    severity="medium",
    event_types=["TaxiEvent"],
    detect_fn=detect_high_value_taxi,
    format_alert_fn=format_high_value_alert,
)
```



## Best Practices for Rule Development

1. **Focus on Physical Impossibility**
   - Check for events that couldn't physically occur (e.g., being in two distant locations simultaneously)
   - Consider realistic travel times between locations based on transportation methods
   - Flag improbable sequences of events (e.g., multiple taxi rides in different cities on the same day)

2. **Minimize False Positives**
   - Implement appropriate thresholds and tolerance for uncertainty
   - Consider different travel speeds (e.g., high-speed rail vs regular train)
   - Account for timezone differences when relevant

3. **Code Quality**
   - Filter events early to reduce processing complexity
   - Group related events before detailed analysis
   - Use clear, descriptive variable names
   - Add comprehensive comments explaining your logic

4. **Alert Quality**
   - Provide specific evidence supporting the detection
   - Include relevant context (e.g., locations, times, amounts)
   - Make alerts actionable by explaining why the pattern is suspicious

## Your Task

Implement a fraud detection rule for "FD-TAXI-MULTICITY-NO-INTERCITY-TRANSPORT	多城市出租车无城际交通	检测用户在不同城市使用出租车，但没有任何城际交通记录（如航班、铁路）解释城市间的移动."


Your solution should include both the detection function and the alert formatting function following the patterns shown in the examples.

Remember to use only fields and methods that exist in the provided entities, and focus on detecting patterns that are physically impossible based on spatiotemporal constraints.