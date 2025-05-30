"""
Convert stop-to-stop arrivals into speeds.
"""
import datetime
import pandas as pd
import sys

from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from segment_speed_utils import (helpers, 
                                 gtfs_schedule_wrangling, 
                                 segment_calcs)
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import SEGMENT_TYPES

def attach_operator_natural_identifiers(
    df: pd.DataFrame, 
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES]
) -> pd.DataFrame:
    """
    For each gtfs_dataset_key-shape_array_key combination,
    re-attach the natural identifiers.
    Return a df with all the identifiers we need during downstream 
    aggregations, such as by route-direction.
    """
    # Get shape_id back
    shape_identifiers = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["shape_array_key", "shape_id"],
        get_pandas = True
    )
    
    route_info = gtfs_schedule_wrangling.attach_scheduled_route_info(
        analysis_date
    )    
    
    df_with_natural_ids = pd.merge(
        df,
        route_info,
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        shape_identifiers,
        on = "shape_array_key",
        how = "inner",
    )
    
    if segment_type in ["rt_stop_times", "modeled_rt_stop_times"]:
        
        trip_stop_cols = [*GTFS_DATA_DICT[segment_type]["trip_stop_cols"]]
        
        stop_pair = helpers.import_scheduled_stop_times(
            analysis_date,
            columns = trip_stop_cols + ["stop_pair", "stop_pair_name"],
            with_direction = True,
            get_pandas = True
        )
        
        df_with_natural_ids2 = df_with_natural_ids.merge(
            stop_pair,
            on = trip_stop_cols
        )
        
    elif segment_type == "speedmap_segments":
        
        trip_stop_cols = [*GTFS_DATA_DICT[segment_type]["trip_stop_cols"]]
        STOP_TIMES_FILE = GTFS_DATA_DICT[segment_type].proxy_stop_times
        
        stop_pair = pd.read_parquet(
            f"{SEGMENT_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet",
            columns = trip_stop_cols + [
                "segment_id", "stop_pair_name", "stop_pair"]
        )
          
        df_with_natural_ids = gtfs_schedule_wrangling.fill_missing_stop_sequence1(
            df_with_natural_ids)
        
        df_with_natural_ids2 = df_with_natural_ids.merge(
            stop_pair,
            on = trip_stop_cols
        )    
        
    return df_with_natural_ids2


def calculate_speed_from_stop_arrivals(
    analysis_date: str, 
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = GTFS_DATA_DICT,
):
    """
    Calculate speed between the interpolated stop arrivals of 
    2 stops. Use current stop to subsequent stop, to match
    with the segments cut by gtfs_segments.create_segments
    """
    dict_inputs = config_path[segment_type]

    trip_cols = ["trip_instance_key"]
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]

    # speedmap segments shoulse the full concatenated one
    if segment_type == "speedmap_segments":
        STOP_ARRIVALS_FILE = f"{dict_inputs['stage3b']}_{analysis_date}"        
    else:
        STOP_ARRIVALS_FILE = f"{dict_inputs['stage3']}_{analysis_date}"

    SPEED_FILE = f"{dict_inputs['stage4']}_{analysis_date}"
    
    start = datetime.datetime.now()
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}.parquet"
    )

    df = segment_calcs.convert_timestamp_to_seconds(
        df, ["arrival_time"]
    ).sort_values(trip_stop_cols).reset_index(drop=True)
    
    df = df.assign(
        subseq_arrival_time_sec = (df.groupby(trip_cols, 
                                             observed=True, group_keys=False)
                                  .arrival_time_sec
                                  .shift(-1)
                                 ),
        subseq_stop_meters = (df.groupby(trip_cols, 
                                        observed=True, group_keys=False)
                             .stop_meters
                             .shift(-1)
                            )
    )

    speed = df.assign(
        meters_elapsed = df.subseq_stop_meters - df.stop_meters, 
        sec_elapsed = df.subseq_arrival_time_sec - df.arrival_time_sec,
    ).pipe(
        segment_calcs.derive_speed, 
        ("stop_meters", "subseq_stop_meters"), 
        ("arrival_time_sec", "subseq_arrival_time_sec")
    ).pipe(
        attach_operator_natural_identifiers, 
        analysis_date, 
        segment_type
    )
        
    speed.to_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}.parquet")
    
    del speed, df
    
    end = datetime.datetime.now()
    logger.info(f"speeds by segment for {segment_type} "
                f"{analysis_date}: {end - start}")
    
    
    return

'''
if __name__ == "__main__":
   
    from segment_speed_utils.project_vars import analysis_date_list

    from dask import delayed, compute
    
    delayed_dfs = [    
        delayed(calculate_speed_from_stop_arrivals)(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        ) for analysis_date in analysis_date_list
    ]
    
    [compute(i)[0] for i in delayed_dfs]
'''