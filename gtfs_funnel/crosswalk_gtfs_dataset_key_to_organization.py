"""
When we publish our downstream outputs, we use the more stable
org_source_record_id and shed our internal modeling keys.

Currently, we save an output with our keys, then an output
without our keys to easily go back to our workflow whenever we get
feedback we get from users, but this is very redundant.
"""
import datetime
import os
import pandas as pd

from calitp_data_analysis.tables import tbls
from siuba import *

from shared_utils import schedule_rt_utils
from segment_speed_utils import helpers
from update_vars import GTFS_DATA_DICT, SCHED_GCS


def create_gtfs_dataset_key_to_organization_crosswalk(
    analysis_date: str
) -> pd.DataFrame:
    """
    For every operator that appears in schedule data, 
    create a crosswalk that links to organization_source_record_id.
    For all our downstream outputs, at various aggregations,
    we need to attach these over and over again.
    """
    df = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "name"],
        get_pandas = True
    ).rename(columns = {"schedule_gtfs_dataset_key": "gtfs_dataset_key"})
    # rename columns because we must use simply gtfs_dataset_key in schedule_rt_utils function
    
    # Get base64_url, organization_source_record_id and organization_name
    crosswalk = schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
        df,
        analysis_date,
        quartet_data = "schedule",
        dim_gtfs_dataset_cols = ["key", "source_record_id", "base64_url"],
        dim_organization_cols = ["source_record_id", "name", "ntd_id_2022"],
        dim_county_geography_cols= ["caltrans_district"], # this is where caltrans_district appears by default
    )

    df_with_org = pd.merge(
        df.rename(columns = {"gtfs_dataset_key": "schedule_gtfs_dataset_key"}),
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )

    return df_with_org

def load_ntd(year: int) -> pd.DataFrame:
    """
    Load NTD Data stored in our warehouse.
    Select certain columns.
    """
    try:
        df = (
        tbls.mart_ntd.dim_annual_agency_information()
        >> filter(_.year == year, _.state == "CA", _._is_current == True)
        >> select(
            _.number_of_state_counties,
            _.primary_uza_name,
            _.density,
            _.number_of_counties_with_service,
            _.state_admin_funds_expended,
            _.service_area_sq_miles,
            _.population,
            _.service_area_pop,
            _.subrecipient_type,
            _.primary_uza_code,
            _.reporter_type,
            _.organization_type,
            _.agency_name,
            _.voms_pt,
            _.voms_do,
            _.ntd_id,
            _.year,
        )
        >> collect()
        )
    except:
        df = (
        tbls.mart_ntd.dim_annual_agency_information()
        >> filter(_.year == year, _.state == "CA", _._is_current == True)
        >> select(
            _.number_of_state_counties,
            _.uza_name,
            _.density,
            _.number_of_counties_with_service,
            _.state_admin_funds_expended,
            _.service_area_sq_miles,
            _.population,
            _.service_area_pop,
            _.subrecipient_type,
            _.reporter_type,
            _.organization_type,
            _.agency_name,
            _.voms_pt,
            _.voms_do,
            _.ntd_id,
            _.year,
            _.primary_uza,
        )
        >> collect()
        )
        df = df.rename(columns = {"uza_name":"primary_uza_name",
                                 "primary_uza":"primary_uza_code"})
    df2 = df.sort_values(by=df.columns.tolist(), na_position="last")
    df3 = df2.groupby("agency_name").first().reset_index()
    
    return df3

def load_mobility(
    cols: list = [
        "agency_name", "counties_served",
        "hq_city", "hq_county",
        "is_public_entity", "is_publicly_operating",
        "funding_sources",
        "on_demand_vehicles_at_max_service",
        "vehicles_at_max_service"
    ]
) -> pd.DataFrame:
    """
    Load mobility data (dim_mobility_mart_providers) 
    from our warehouse.
    """
    df = (
        tbls.mart_transit_database.dim_mobility_mart_providers()
        >> select(*cols)
        >> collect()
    )
    
    df2 = df.sort_values(
        by=["on_demand_vehicles_at_max_service","vehicles_at_max_service"], 
        ascending = [False, False]
    )
    
    df3 = df2.groupby('agency_name').first().reset_index()
    
    return df3

def merge_ntd_mobility(year:int)->pd.DataFrame:
    """
    Merge NTD (dim_annual_ntd_agency_information) with 
    mobility providers (dim_mobility_mart_providers)
    and dedupe and keep 1 row per agency.
    """
    ntd = load_ntd(year)
    mobility = load_mobility()
    
    m1 = pd.merge(
        mobility,
        ntd,
        how="inner",
        on="agency_name"
    )

    m1 = m1.drop_duplicates(
        subset="agency_name"
    ).reset_index(
        drop=True
    ).drop(columns = "agency_name")
    
    # Wherever possible, allow nullable integers. These columns are integers, but can be
    # missing if we don't find corresponding NTD info
    integrify_cols = [
        "number_of_state_counties", "number_of_counties_with_service", 
        "service_area_sq_miles", "service_area_pop",
        "on_demand_vehicles_at_max_service", "vehicles_at_max_service",
        "voms_pt", "voms_do", "year",
    ]
    m1[integrify_cols] = m1[integrify_cols].astype("Int64")

    return m1

if __name__ == "__main__":

    from update_vars import analysis_date_list, ntd_latest_year
    
    EXPORT = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk
    start = datetime.datetime.now()
    
    for analysis_date in analysis_date_list:
        t0 = datetime.datetime.now()
        df = create_gtfs_dataset_key_to_organization_crosswalk(
            analysis_date
        )
        
        # Add some NTD data: if I want to delete this, simply take out
        # ntd_df and the crosswalk_df merge.
        ntd_df = merge_ntd_mobility(ntd_latest_year)
        
        crosswalk_df = pd.merge(
            df,
            ntd_df.rename(columns = {"ntd_id": "ntd_id_2022"}),
            on = ["ntd_id_2022"],
            how = "left"
        )

        # Drop ntd_id from ntd_df to avoid confusion
        crosswalk_df = crosswalk_df.drop(columns = ["ntd_id_2022"])
        
        # Drop duplicates since we're getting a lot. 
        crosswalk_df.to_parquet(
            f"{SCHED_GCS}{EXPORT}_{analysis_date}.parquet"
        )
        t1 = datetime.datetime.now()
        print(f"finished {analysis_date}: {t1-t0}")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")

 