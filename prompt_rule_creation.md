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

## Available Helper Functions

Our rule engine provides these helper functions through the `rule` object:

### Spatial Functions
```python
# Calculate distance between locations in kilometers
rule.get_distance(location1: Location, location2: Location) -> Optional[float]

# Check if locations are within specified distance
rule.is_within_distance(location1: Location, location2: Location, max_distance_km: float = 1.0) -> bool

# Calculate travel time between locations in hours
rule.calculate_travel_time(location1: Location, location2: Location, speed_kmh: Optional[float] = 120) -> Optional[float]

# Check if locations are in the same city
rule.is_same_city(location1: Location, location2: Location) -> bool
```

### Temporal Functions
```python
# Calculate time difference in specified unit (seconds, minutes, hours, days)
rule.time_difference(time1: datetime, time2: datetime, unit: str = 'hours') -> float

# Check if datetime falls within specific time range
rule.is_within_time_range(dt: datetime, start_hours: float, end_hours: float) -> bool

# Check if time windows overlap
rule.do_time_intervals_overlap(interval1: TimeWindow, interval2: TimeWindow) -> bool

# Get overlap duration between time windows in specified unit
rule.get_overlap_duration(interval1: TimeWindow, interval2: TimeWindow, unit: str = 'hours') -> float

# Check if datetime is during business hours on weekday
rule.is_business_hours(dt: datetime, start_hour: float = 9.0, end_hour: float = 18.0) -> bool

# Check if datetime is after business hours on weekday
rule.is_after_hours(dt: datetime, business_end_hour: float = 18.0) -> bool

# Check if datetime is on weekend
rule.is_weekend(dt: datetime) -> bool
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

### Example 2: Hotel Stay Without Travel
This rule detects when someone claims hotel expenses without corresponding travel events:

```python
from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent, HotelEvent, FlightEvent, RailwayEvent
from expensecbr.fde import create_time_window_rule

def detect_hotel_without_travel(rule, events, context):
    """
    Detect when a user claims hotel expenses in a different city without travel records.
    
    This rule identifies potentially fraudulent hotel bookings where:
    1. The hotel is in a different city from the user's normal work location
    2. There are no flight or railway events that would justify travel to that city
    3. The hotel stay is of sufficient duration to be suspicious
    """
    # Filter for hotel events
    hotel_events = [e for e in events if isinstance(e, HotelEvent)]
    if not hotel_events:
        return False
        
    # Filter for travel events
    travel_events = [
        e for e in events if isinstance(e, FlightEvent) or isinstance(e, RailwayEvent)
    ]
    
    # Get work locations from context
    work_locations = context.get("default_work_locations", {})
    
    suspicious_patterns = []
    
    for hotel_event in hotel_events:
        user_id = hotel_event.user_id
        hotel_city = hotel_event.location.city
        
        # Skip if we can't determine hotel city
        if not hotel_city:
            continue
            
        # Get user's work location
        user_work_loc = work_locations.get(user_id)
        work_city = user_work_loc.city if user_work_loc else None
        
        # Skip if hotel is in work city or we don't know work city
        if not work_city or hotel_city == work_city:
            continue
            
        # Calculate hotel stay duration
        stay_duration = rule.time_difference(
            hotel_event.time_window.earliest_start,
            hotel_event.time_window.latest_end,
            unit="days"
        )
        
        # Skip if stay is too short to be suspicious
        if stay_duration < 1:
            continue
            
        # Check for justifying travel events
        justified = False
        for travel_event in travel_events:
            if travel_event.user_id != user_id:
                continue
                
            # Check if travel is to the hotel city
            if not hasattr(travel_event, "to_location") or not travel_event.to_location:
                continue
                
            if travel_event.to_location.city != hotel_city:
                continue
                
            # Check timing is reasonable
            time_diff = rule.time_difference(
                travel_event.time_window.latest_end,
                hotel_event.time_window.earliest_start,
                unit="hours"
            )
            
            if time_diff <= 24:
                justified = True
                break
                
        # Flag if not justified
        if not justified:
            suspicious_patterns.append({
                "primary_event_id": hotel_event.event_id,
                "user_id": user_id,
                "hotel_city": hotel_city,
                "work_city": work_city,
                "stay_duration": stay_duration,
                "amount": hotel_event.amount
            })
    
    return suspicious_patterns if suspicious_patterns else False

def format_hotel_alert(rule, events, extra_data, context):
    """Format alert for hotel stay without travel"""
    # Find primary event
    primary_id = extra_data.get("primary_event_id")
    event = next((e for e in events if e.event_id == primary_id), None)
    
    if not event:
        return {"title": "Suspicious Hotel Stay", "details": "Details unavailable"}
    
    title = f"Hotel Stay Without Travel: {extra_data.get('hotel_city')} ({event.amount:.2f} yuan)"
    
    details = (
        f"User {event.user_name} ({event.user_id}) claimed expenses for a hotel stay in "
        f"{extra_data.get('hotel_city')}, which is different from their work city "
        f"({extra_data.get('work_city')}). No corresponding travel records (flights or trains) "
        f"were found to justify this hotel stay.\n\n"
        f"Stay duration: {extra_data.get('stay_duration', 0):.1f} days\n"
        f"Amount: {event.amount:.2f} yuan\n"
        f"Hotel: {event.hotel_name}\n"
        f"Room type: {event.room_type}"
    )
    
    return {"title": title, "details": details}

# Create the rule
hotel_without_travel_rule = create_time_window_rule(
    rule_id="FD-HOTEL-NO-TRAVEL",
    title="Hotel Stay Without Travel Records",
    description="Detects when a user claims hotel expenses in a city different from their work location without travel records",
    severity="medium",
    event_types=["HotelEvent", "FlightEvent", "RailwayEvent"],
    detect_fn=detect_hotel_without_travel,
    format_alert_fn=format_hotel_alert,
    window_days=3  # Look at events within 3 days
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

Implement a fraud detection rule for <Narrative Descrisption of the Pattern Or Leave as Empty for Rule Generation From Scratch>


Your solution should include both the detection function and the alert formatting function following the patterns shown in the examples.

Remember to use only fields and methods that exist in the provided entities, and focus on detecting patterns that are physically impossible based on spatiotemporal constraints.