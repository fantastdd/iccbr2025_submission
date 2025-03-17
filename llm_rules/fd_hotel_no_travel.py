from typing import Dict, Any, List
from expensecbr.base import (
    TrajectoryEvent,
    HotelEvent,
    FlightEvent,
    RailwayEvent,
    TransportEvent,
)
from expensecbr.fde import create_time_window_rule
from datetime import timedelta


def detect_hotel_stay_without_travel(rule, events, context):
    """
    Detect when a user claims hotel expenses in a city different from their work location
    without corresponding travel events (flight/railway) to justify the hotel stay.

    This rule identifies potentially fraudulent hotel bookings where:
    1. The hotel is in a different city from the user's normal work location
    2. There are no flight or railway events that would justify travel to that city
    3. The hotel stay is of sufficient duration to be suspicious (not just a day trip)
    """
    # Filter for hotel events
    hotel_events = [e for e in events if isinstance(e, HotelEvent)]

    # If no hotel events, nothing to analyze
    if not hotel_events:
        return False

    # Filter for travel events (flights and railways)
    travel_events = [
        e for e in events if isinstance(e, FlightEvent) or isinstance(e, RailwayEvent)
    ]

    # Get necessary parameters from context
    default_work_location = context.get("default_work_locations", {})
    min_stay_nights = context.get("min_suspicious_stay_nights", 1)

    suspicious_patterns = []

    # Analyze each hotel event
    for hotel_event in hotel_events:
        user_id = hotel_event.user_id
        hotel_city = hotel_event.location.city if hotel_event.location else None

        # Skip if we can't determine hotel city
        if not hotel_city:
            continue

        # Get user's default work location from context
        user_work_loc = default_work_location.get(user_id)
        user_work_city = user_work_loc.city if user_work_loc else None

        # If we don't know user's work city, skip this event
        if not user_work_city:
            continue

        # Skip if hotel is in user's work city (not suspicious)
        if hotel_city == user_work_city:
            continue

        # Calculate hotel stay duration in nights
        stay_duration = rule.time_difference(
            hotel_event.time_window.earliest_start,
            hotel_event.time_window.latest_end,
            unit="days",
        )

        # Skip if stay is too short to be suspicious
        if stay_duration < min_stay_nights:
            continue

        # Look for travel events that would justify this hotel stay
        justified = False
        matching_travel_events = []

        for travel_event in travel_events:
            # Skip if not the same user
            if travel_event.user_id != user_id:
                continue

            # Check if this is travel to the hotel city using to_location
            # With our new structure, all transport events have to_location
            if not hasattr(travel_event, "to_location") or not travel_event.to_location:
                continue

            travel_to_city = travel_event.to_location.city == hotel_city

            # Skip if not travel to the relevant city
            if not travel_to_city:
                continue

            # Check if travel event justifies hotel stay based on timing
            travel_time = travel_event.time_window
            hotel_time = hotel_event.time_window

            # Travel should be before hotel check-in or shortly after
            time_diff = rule.time_difference(
                travel_time.latest_end, hotel_time.earliest_start, unit="hours"
            )

            # If travel ends before hotel check-in or within 24 hours after check-in
            if time_diff <= 24:
                justified = True
                matching_travel_events.append(travel_event.event_id)
                break

        # If no justifying travel events were found, flag as suspicious
        if not justified:
            suspicious_patterns.append(
                {
                    "primary_event_id": hotel_event.event_id,
                    "user_id": user_id,
                    "user_name": hotel_event.user_name,
                    "department": hotel_event.department,
                    "hotel_name": hotel_event.hotel_name,
                    "hotel_city": hotel_city,
                    "work_city": user_work_city,
                    "stay_duration": stay_duration,
                    "amount": hotel_event.amount,
                    "check_in": hotel_event.time_window.earliest_start,
                    "check_out": hotel_event.time_window.latest_end,
                    "guest_name": hotel_event.guest_name,
                    "guest_type": hotel_event.guest_type,
                    "hotel_level": hotel_event.hotel_level,
                    "room_type": hotel_event.room_type,
                }
            )

    return suspicious_patterns if suspicious_patterns else False


def format_hotel_stay_alert(rule, events, extra_data, context):
    """Format alert details for hotel stay without travel justification"""
    # Get relevant data from extra_data
    user_name = extra_data.get("user_name", "Unknown")
    user_id = extra_data.get("user_id", "Unknown")
    department = extra_data.get("department", "Unknown")
    hotel_name = extra_data.get("hotel_name", "Unknown")
    hotel_city = extra_data.get("hotel_city", "Unknown")
    work_city = extra_data.get("work_city", "Unknown")
    stay_duration = extra_data.get("stay_duration", 0)
    amount = extra_data.get("amount", 0)
    guest_name = extra_data.get("guest_name", "Unknown")
    guest_type = extra_data.get("guest_type", "Unknown")
    hotel_level = extra_data.get("hotel_level", "Unknown")
    room_type = extra_data.get("room_type", "Unknown")

    # Format dates
    check_in = extra_data.get("check_in", "Unknown")
    check_out = extra_data.get("check_out", "Unknown")

    if check_in != "Unknown":
        check_in_str = check_in.strftime("%Y-%m-%d")
    else:
        check_in_str = "Unknown"

    if check_out != "Unknown":
        check_out_str = check_out.strftime("%Y-%m-%d")
    else:
        check_out_str = "Unknown"

    title = f"Hotel Stay Without Travel Justification: {hotel_city} ({amount:.2f} yuan)"

    details = (
        f"User {user_name} ({user_id}) from {department} claimed expenses for a {stay_duration:.1f}-night "
        f"hotel stay at {hotel_name} in {hotel_city} from {check_in_str} to {check_out_str} "
        f"costing {amount:.2f} yuan.\n\n"
        f"This stay is potentially suspicious because:\n"
        f"- The hotel is in {hotel_city}, which is different from the user's work city of {work_city}\n"
        f"- No corresponding travel (flight or train) was found to justify travel to {hotel_city}\n"
        f"- The stay duration of {stay_duration:.1f} nights exceeds typical day-trip length\n\n"
        f"Additional details:\n"
        f"- Guest name: {guest_name}\n"
        f"- Guest type: {guest_type}\n"
        f"- Hotel level: {hotel_level}\n"
        f"- Room type: {room_type}"
    )

    return {"title": title, "details": details}


# Create the rule using the factory function
hotel_without_travel_rule = create_time_window_rule(
    rule_id="FD-HOTEL-NO-TRAVEL",
    title="Hotel Stay Without Travel Justification",
    description="Detects when a user claims hotel expenses in a city different from their work location without corresponding travel events to justify the stay",
    severity="high",
    event_types=["HotelEvent", "FlightEvent", "RailwayEvent"],
    window_days=3,
    detect_fn=detect_hotel_stay_without_travel,
    format_alert_fn=format_hotel_stay_alert,
)
