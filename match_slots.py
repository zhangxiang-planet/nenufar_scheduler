import pandas as pd
from datetime import datetime, timedelta

def load_data(plan_file, slots_file):
    """
    Load observational plan and allocated slots from CSV files.
    
    Args:
        plan_file (str): Path to the observational plan CSV file.
        slots_file (str): Path to the allocated slots CSV file.
    
    Returns:
        tuple: DataFrames for observational plan and allocated slots.
    """
    plan_df = pd.read_csv(plan_file)
    plan_df["StartTime"] = pd.to_datetime(plan_df["StartTime"])
    plan_df["StopTime"] = pd.to_datetime(plan_df["StopTime"])
    
    slots_df = pd.read_csv(slots_file)
    slots_df["startTime"] = pd.to_datetime(slots_df["startTime"])
    slots_df["stopTime"] = pd.to_datetime(slots_df["stopTime"])
    
    return plan_df, slots_df

def cross_match_observations(plan_df, slots_df, hour_tolerance=2, day_tolerance=3):
    """
    Cross-match observations with allocated slots, ensuring each slot is used only once.
    
    Args:
        plan_df (pd.DataFrame): Observational plan DataFrame.
        slots_df (pd.DataFrame): Allocated slots DataFrame.
        hour_tolerance (int): Hour-of-the-day tolerance for matching.
        day_tolerance (int): Day tolerance for matching.
    
    Returns:
        pd.DataFrame: Cross-matched DataFrame.
    """
    matches = []
    used_slots = set()

    for _, obs in plan_df.iterrows():
        # Define date range tolerance
        start_window = obs["StartTime"] - pd.Timedelta(days=day_tolerance)
        end_window = obs["StopTime"] + pd.Timedelta(days=day_tolerance)

        # Filter slots by date range and exclude already used slots
        potential_slots = slots_df[
            (slots_df["startTime"] >= start_window) &
            (slots_df["stopTime"] <= end_window)
        ].copy()
        potential_slots = potential_slots[~potential_slots.index.isin(used_slots)]

        # Further filter by time-of-day tolerance
        filtered_slots = []
        for slot_idx, slot in potential_slots.iterrows():
            obs_start_time = timedelta(hours=obs["StartTime"].hour, minutes=obs["StartTime"].minute)
            obs_end_time = timedelta(hours=obs["StopTime"].hour, minutes=obs["StopTime"].minute)
            slot_start_time = timedelta(hours=slot["startTime"].hour, minutes=slot["startTime"].minute)
            slot_end_time = timedelta(hours=slot["stopTime"].hour, minutes=slot["stopTime"].minute)

            start_time_difference = abs((obs_start_time - slot_start_time).total_seconds() / 3600)
            end_time_difference = abs((obs_end_time - slot_end_time).total_seconds() / 3600)

            if start_time_difference <= hour_tolerance or end_time_difference <= hour_tolerance:
                filtered_slots.append((slot_idx, slot))

        # If potential slots exist, select the best match
        if filtered_slots:
            filtered_slots_df = pd.DataFrame([slot[1] for slot in filtered_slots])
            filtered_slots_df["time_diff"] = abs(
                (filtered_slots_df["startTime"] - obs["StartTime"]).dt.total_seconds()
            )
            best_slot_idx = filtered_slots_df["time_diff"].idxmin()
            best_slot = slots_df.loc[best_slot_idx]

            # Add to matches and mark slot as used
            match = obs.to_dict()
            match.update(best_slot.to_dict())
            matches.append(match)
            used_slots.add(best_slot_idx)
        else:
            # No match found for this observation
            match = obs.to_dict()
            for col in slots_df.columns:
                if col not in match:
                    match[col] = None
            matches.append(match)

    return pd.DataFrame(matches)

def adjust_observational_times(matches_df):
    """
    Adjust the allocated slot times to calculate actual observational times
    based on the observational plan's hours of the day, handling midnight cases.
    
    Args:
        matches_df (pd.DataFrame): Cross-matched DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with adjusted actual start and end times.
    """
    adjusted_start_times = []
    adjusted_end_times = []

    for _, row in matches_df.iterrows():
        plan_start_hour = row["StartTime"].hour
        plan_start_minute = row["StartTime"].minute
        plan_end_hour = row["StopTime"].hour
        plan_end_minute = row["StopTime"].minute

        slot_start = row["startTime"]
        slot_end = row["stopTime"]

        if pd.notna(slot_start) and pd.notna(slot_end):
            actual_start = slot_start.replace(hour=plan_start_hour, minute=plan_start_minute)
            if plan_end_hour < plan_start_hour or (plan_end_hour == 0 and plan_start_hour != 0):
                actual_end = (slot_start + pd.Timedelta(days=1)).replace(hour=plan_end_hour, minute=plan_end_minute)
            else:
                actual_end = slot_start.replace(hour=plan_end_hour, minute=plan_end_minute)

            actual_start = max(actual_start, slot_start)
            actual_end = min(actual_end, slot_end)

            if actual_start < actual_end:
                adjusted_start_times.append(actual_start)
                adjusted_end_times.append(actual_end)
            else:
                adjusted_start_times.append(None)
                adjusted_end_times.append(None)
        else:
            adjusted_start_times.append(None)
            adjusted_end_times.append(None)

    matches_df["ActualStartTime"] = adjusted_start_times
    matches_df["ActualEndTime"] = adjusted_end_times
    return matches_df

def main(plan_file, slots_file, output_file):
    """
    Main function to process observational plan and allocated slots, and output actual observational times.
    
    Args:
        plan_file (str): Path to the observational plan CSV file.
        slots_file (str): Path to the allocated slots CSV file.
        output_file (str): Path to save the output CSV file.
    """
    plan_df, slots_df = load_data(plan_file, slots_file)
    matches_df = cross_match_observations(plan_df, slots_df)
    adjusted_matches_df = adjust_observational_times(matches_df)
    adjusted_matches_df.to_csv(output_file, index=False)
    print(f"Actual observational times saved to {output_file}")

# Example usage:
# main("observational_plan.csv", "allocated_slots.csv", "actual_observational_times.csv")