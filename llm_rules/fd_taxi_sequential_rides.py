from typing import Dict, Any, List
from expensecbr.base import TrajectoryEvent, TaxiEvent
from expensecbr.fde import create_daily_rule
from datetime import timedelta


def detect_sequential_taxi_rides(rule, events, context):
    """
    Detect when a user takes multiple sequential taxi rides in a single day with:
    1. Short intervals between rides (suggesting they could have been a single trip)
    2. Total amount exceeding typical approval thresholds

    This pattern may indicate expense splitting to avoid approval thresholds
    or to make high expenses appear as multiple smaller legitimate expenses.
    """
    # Filter for taxi events only
    taxi_events = [e for e in events if isinstance(e, TaxiEvent)]

    # Skip if fewer than 3 taxi rides (not suspicious enough)
    if len(taxi_events) < 3:
        return False

    # Group taxi events by user_id
    user_events = {}
    for event in taxi_events:
        if event.user_id not in user_events:
            user_events[event.user_id] = []
        user_events[event.user_id].append(event)

    suspicious_patterns = []

    # Get thresholds from context or use defaults
    time_threshold = context.get(
        "sequential_taxi_time_threshold", 0.5
    )  # 30 minutes in hours
    amount_threshold = context.get("sequential_taxi_amount_threshold", 150.0)  # Yuan
    min_rides = context.get("sequential_taxi_min_rides", 3)

    # Analyze events for each user
    for user_id, user_taxi_events in user_events.items():
        # Skip if not enough events from this user
        if len(user_taxi_events) < min_rides:
            continue

        # Sort by start time
        sorted_events = sorted(
            user_taxi_events, key=lambda e: e.time_interval.earliest_start
        )

        # Find sequences of rides with short intervals
        sequences = []
        current_sequence = [sorted_events[0]]

        for i in range(1, len(sorted_events)):
            current_event = sorted_events[i]
            previous_event = sorted_events[i - 1]

            # Check if rides are in sequence by time and location
            time_between = rule.time_difference(
                previous_event.time_interval.latest_end,
                current_event.time_interval.earliest_start,
                unit="hours",
            )

            location_match = rule.is_within_distance(
                previous_event.to_location,
                current_event.from_location,
                max_distance_km=2.0,
            )

            # If time between rides is short and locations match
            if 0 <= time_between <= time_threshold and location_match:
                current_sequence.append(current_event)
            else:
                # If we have enough events in sequence, save it
                if len(current_sequence) >= min_rides:
                    sequences.append(list(current_sequence))
                # Start a new sequence
                current_sequence = [current_event]

        # Check the last sequence
        if len(current_sequence) >= min_rides:
            sequences.append(list(current_sequence))

        # Analyze each sequence for suspicious patterns
        for sequence in sequences:
            total_amount = sum(event.amount for event in sequence)

            # If total amount exceeds threshold, flag as suspicious
            if total_amount >= amount_threshold:
                # Calculate total distance traveled in the sequence
                total_distance = 0
                for i in range(len(sequence)):
                    event = sequence[i]
                    if hasattr(event, "from_location") and hasattr(
                        event, "to_location"
                    ):
                        distance = rule.get_distance(
                            event.from_location, event.to_location
                        )
                        total_distance += distance

                time_span = rule.time_difference(
                    sequence[0].time_interval.earliest_start,
                    sequence[-1].time_interval.latest_end,
                    unit="hours",
                )

                suspicious_patterns.append(
                    {
                        "primary_event_id": sequence[-1].event_id,
                        "related_event_ids": [e.event_id for e in sequence],
                        "user_id": user_id,
                        "user_name": sequence[0].user_name,
                        "department": sequence[0].department,
                        "total_amount": total_amount,
                        "ride_count": len(sequence),
                        "total_distance": total_distance,
                        "time_span": time_span,
                        "avg_amount_per_ride": total_amount / len(sequence),
                        "sequence_start": sequence[0].time_interval.earliest_start,
                        "sequence_end": sequence[-1].time_interval.latest_end,
                    }
                )

    return suspicious_patterns if suspicious_patterns else False


def format_sequential_taxi_alert(rule, events, extra_data, context):
    """Format alert details for sequential taxi rides detection"""
    # Get relevant data from extra_data
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    total_amount = extra_data.get("total_amount", 0)
    ride_count = extra_data.get("ride_count", 0)
    time_span = extra_data.get("time_span", 0)
    total_distance = extra_data.get("total_distance", 0)

    # Format date
    date_str = extra_data.get("sequence_start", "Unknown")
    if date_str != "Unknown":
        date_str = date_str.strftime("%Y-%m-%d")

    # Generate detailed description of individual rides
    ride_details = []
    related_ids = extra_data.get("related_event_ids", [])
    related_events = [e for e in events if e.event_id in related_ids]

    for event in sorted(related_events, key=lambda e: e.time_interval.earliest_start):
        from_loc = (
            f"{event.from_location.city} {event.from_location.specific_location}"
            if hasattr(event, "from_location")
            else "Unknown"
        )
        to_loc = (
            f"{event.to_location.city} {event.to_location.specific_location}"
            if hasattr(event, "to_location")
            else "Unknown"
        )

        time_str = event.time_interval.earliest_start.strftime("%H:%M")
        ride_details.append(
            f"- {time_str}: {from_loc} → {to_loc} ({event.amount:.2f} yuan)"
        )

    ride_details_str = "\n".join(ride_details)

    title = f"Sequential Taxi Rides: {total_amount:.2f} yuan across {ride_count} rides"

    details = (
        f"User {user_name} ({user_id}) from {department} took {ride_count} sequential "
        f"taxi rides on {date_str} over a period of {time_span:.2f} hours, "
        f"covering approximately {total_distance:.2f} km. "
        f"The total cost was {total_amount:.2f} yuan, which may indicate "
        f"expense splitting to avoid approval thresholds.\n\n"
        f"Sequential ride details:\n{ride_details_str}"
    )

    return {"title": title, "details": details}


# Create the rule using the factory function
sequential_taxi_rule = create_daily_rule(
    rule_id="FD-TAXI-SEQUENTIAL-RIDES",
    title="Sequential Taxi Rides Pattern",
    description="Detects when a user takes multiple sequential taxi rides in a single day with short intervals between rides and high total cost, potentially indicating expense splitting to avoid approval thresholds",    
    event_types=["TaxiEvent"],
    detect_fn=detect_sequential_taxi_rides,
    format_alert_fn=format_sequential_taxi_alert,
)
