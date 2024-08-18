
from sparksession import SparkSessionSingleton
from error_handling import handle_error
from data_ingestion.data_ingestion import read_data
from analytics.car_crash_analysis import CarCrashAnalysis
import yaml

@handle_error
def main():
    spark = SparkSessionSingleton.get_spark_session()

    charges_df, damages_df, endorse_df, Primary_Person_use_df, restrict_df, Units_use_df = read_data(spark)
    
    no_of_male_accidents = CarCrashAnalysis.count_male_accidents(Primary_Person_use_df)
    
    no_of_2_wheeler_accidents = CarCrashAnalysis.count_2_wheeler_accidents(Units_use_df)
    
    driver_dead_df = CarCrashAnalysis.top_5_vehicle_makes_for_fatal_crashes_without_airbags(Primary_Person_use_df, Units_use_df)
    
    no_of_hit_and_run_with_valid_licenses = CarCrashAnalysis.count_hit_and_run_with_valid_licenses(
        Primary_Person_use_df, 
        Units_use_df
    )

    
#     top_crash_state_with_no_female_accident = CarCrashAnalysis.get_state_with_no_female_accident(Primary_Person_use_df)
    
    top_vehicle_contributing_to_injuries_df = CarCrashAnalysis.get_top_vehicle_contributing_to_injuries(Units_use_df)
    
    top_ethnic_ug_crash_for_each_body_style_df = CarCrashAnalysis.get_top_ethnic_ug_crash_for_each_body_style(
        Primary_Person_use_df, 
        Units_use_df
    )

    
    top_5_zip_codes_with_alcohols_as_cf_for_crash_df = CarCrashAnalysis.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
        Primary_Person_use_df, 
        Units_use_df
    )

    
    no_of_crash_ids_with_no_damage = CarCrashAnalysis.get_crash_ids_with_no_damage(Units_use_df)
    
    get_top_5_vehicle_brand_df = CarCrashAnalysis.get_top_5_vehicle_brand(charges_df, Primary_Person_use_df, Units_use_df)
        

if __name__ == "__main__":
    main()
    