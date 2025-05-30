"""
Cut stop-to-stop segments for all trips.
Use one of gtfs_segments functions to do it...
it cuts the segments, particularly loop_or_inlining
shapes better at the edges.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gtfs_segments
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import gtfs_schedule_wrangling, helpers
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS
from segment_speed_utils.project_vars import PROJECT_CRS 
                                             

def stop_times_with_shape(
    analysis_date: str
) -> dg.GeoDataFrame: 
    """
    Filter down to trip_instance_keys present in vp,
    and attach stop_times and shapes.
    Set up this df the way we need to use gtfs_segments.create_segments.
    """
    rt_trips = helpers.import_unique_vp_trips(analysis_date)
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key",
                   "shape_array_key",
                   "stop_id", "stop_sequence", "geometry"],
        filters = [[("trip_instance_key", "in", rt_trips)]],
        with_direction = True,
        get_pandas = True,
        crs = WGS84
    )
    stop_times = dg.from_geopandas(stop_times, npartitions=4)
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = WGS84,
        get_pandas = True
    ).dropna(subset="geometry")
    
    df = dd.merge(
        stop_times,
        shapes,
        on = "shape_array_key",
        how = "inner"
    ).rename(columns = {
        "geometry_x": "start",
        "geometry_y": "geometry",
    }).pipe(
        gtfs_schedule_wrangling.gtfs_segments_rename_cols,
        natural_identifier = True
    ).dropna(
        subset="geometry"
    ).reset_index(drop=True).set_geometry("geometry")
    
    # Add a dummy arrival_time that is needed in gtfs_segments that is not NaT
    # or else it'll throw error in gtfs_segments.create_segments. Use zero instead.
    df = df.assign(
        arrival_time = 0
    )
    
    return df


def cut_stop_segments(analysis_date: str) -> gpd.GeoDataFrame:
    ddf = stop_times_with_shape(analysis_date)
    
    # This is stop_times for all rt_trips, could be a lot
    # so let's partition it with a lot of npartitions
    ddf = ddf.repartition(npartitions=150).persist()
        
    renamed_ddf = ddf.rename(columns = {
        "stop_id": "stop_id1",
        "arrival_time": "arrival_time1"
    })
    orig_dtypes = renamed_ddf.dtypes.to_dict()

    segments = ddf.map_partitions(
        gtfs_segments.gtfs_segments.create_segments,
        meta = {
            **orig_dtypes,
            "snap_start_id": "int", 
            "stop_id2": "str", 
            "end": "geometry",
            "snap_end_id": "int", 
            "arrival_time2": "int",
            "segment_id": "str"
        },
        align_dataframes = False
    )
    
    # We don't need several of these columns, esp 3 geometry columns
    segments = (segments.drop(
        columns = [
            "start", "end", 
            "snap_start_id", "snap_end_id",
            "arrival_time1", "arrival_time2"
        ]
    ).pipe(
        gtfs_schedule_wrangling.gtfs_segments_rename_cols,
        natural_identifier = False
    ).set_geometry("geometry")
     .set_crs(WGS84)
     .to_crs(PROJECT_CRS)
     .compute()
     )
    
    # Add stop_pair now
    segments = segments.assign(
        stop_pair = segments.stop_id1 + "__" + segments.stop_id2
    )
    
    return segments
    

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/cut_stop_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")    
    
    RT_DICT = GTFS_DATA_DICT.rt_stop_times
    
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()
        
        SEGMENT_FILE = RT_DICT["segments_file"]      
        
        segments = cut_stop_segments(analysis_date)
        
        shape_to_route = helpers.import_scheduled_trips(
            analysis_date,
            columns = ["gtfs_dataset_key", "shape_array_key", 
                       "route_id", "direction_id"]
        )
        
        segments = pd.merge(
            segments,
            shape_to_route,
            on = "shape_array_key",
            how = "inner"
        )
        
        # Create uuid by concatenating several columns
        # Can be easier to use if we want to do segment-specific things
        # and find our way back to the full columns later
        segments = segments.assign(
            segment_uuid = segments.schedule_gtfs_dataset_key.str.cat(
                segments[["route_id", "direction_id", "segment_id"]].astype(str), 
                sep = "__")
        )
                
        utils.geoparquet_gcs_export(
            segments,
            SEGMENT_GCS,
            f"{SEGMENT_FILE}_{analysis_date}"
        )    
        
        del segments, shape_to_route
    
        end = datetime.datetime.now()
        logger.info(f"cut segments {analysis_date}: {end - start}")