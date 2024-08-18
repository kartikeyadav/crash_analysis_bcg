from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, countDistinct, when, desc, row_number, sum, dense_rank, regexp_extract, collect_list
from error_handling import handle_error
import yaml
import pandas

# Load configuration
try:
    with open("C:/Users/user/Downloads/src/CrashAnalysisProject/src/config.yml", 'r') as file:
        config = yaml.safe_load(file)
except Exception as e:
    print(f"Error loading config file: {str(e)}")
    raise

# Extract paths from config
try:
    output_path = config["files"]["outputpath"]
    sol1 = config["files"]["sol1"]
    sol2 = config["files"]["sol2"]  
    sol3 = config["files"]["sol3"]
    sol4 = config["files"]["sol4"]
    sol5 = config["files"]["sol5"]
    sol6 = config["files"]["sol6"]
    sol7 = config["files"]["sol7"]
    sol8 = config["files"]["sol8"]
    sol9 = config["files"]["sol9"]
    sol10 = config["files"]["sol10"]
except KeyError as e:
    print(f"Missing key in config file: {str(e)}")
    raise

class CarCrashAnalysis:
    def __init__(self, spark):
        """
        Initializes the analysis class with the provided Spark session.
        
        Parameters:
        - spark: SparkSession object
        """
        self.spark = spark
    @handle_error
    def count_male_accidents(Primary_Person_use):
        """
        Finds the number of crashes (accidents) in which the number of males killed is greater than 2.
        
        Parameters:
        - Primary_Person_use: DataFrame containing primary person data.
        
        Returns:
        - int: The count of crashes in which the number of males killed is greater than 2 (Ans=0)
        """
        df = Primary_Person_use.groupBy("CRASH_ID")\
            .agg(count(when((col("DEATH_CNT") == 1) & (col("PRSN_GNDR_ID") == "MALE"), 1)).alias("male_death_cnt"))\
            .filter(col("male_death_cnt") > 2)

#       df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol1}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol1}", index=False)
        
        return df.count()

    @handle_error
    def count_2_wheeler_accidents(Units_use):
        """
        Finds crashes where the vehicle body type was two wheelers.
        
        Parameters:
        - Units_use: DataFrame containing units data.
        
        Returns:
        - int: The count of crashes involving 2-wheeler vehicles.(Ans=781)
        """
        df = Units_use.filter(col("VEH_BODY_STYL_ID") == "MOTORCYCLE")
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol2}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol2}", index=False)

        return df.count()

    @handle_error
    def top_5_vehicle_makes_for_fatal_crashes_without_airbags(Primary_Person_use, Units_use):
        """
        Determines the top 5 Vehicle Makes of the cars involved in crashes where the driver died and airbags did not deploy.
        
        Parameters:
        - Primary_Person_use: DataFrame containing data of Primary_Person_use.
        - Units_use: DataFrame containing units data.
        
        Returns:
        - DataFrame: The top 5 vehicle makes involved in fatal crashes without airbag deployment.(Ans=CHEVROLET, FORD, NISSAN, DODGE, HONDA)
        """
        window_spec = Window.orderBy(desc("total_dead"))
        
        driver_dead = Primary_Person_use.filter(\
            (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") &\
            (col("DEATH_CNT") == 1) &\
            (col("PRSN_TYPE_ID") == "DRIVER")\
        )


        df = driver_dead.join(Units_use.select("CRASH_ID", "UNIT_NBR", "VEH_MAKE_ID"), on=["CRASH_ID", "UNIT_NBR"])\
            .groupBy("VEH_MAKE_ID")\
            .agg(count("VEH_MAKE_ID").alias("total_dead"))\
            .withColumn("rnk", row_number().over(window_spec))\
            .filter(col("rnk") <= 5)
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol3}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol3}", index=False)
        
        return df

    @handle_error
    def count_hit_and_run_with_valid_licenses(Primary_Person_use, Units_use):
        """
        Determines the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
        
        Parameters:
        - Primary_Person_use: DataFrame containing primary person data.
        - Units_use: DataFrame containing units data.
        
        Returns:
        - int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.(Ans= 2602)
        """
        hit_n_run = Primary_Person_use.filter((col("PRSN_TYPE_ID") == "DRIVER") &
                                              (col("DRVR_LIC_TYPE_ID").isin(["COMMERCIAL DRIVER LIC.", "DRIVER LICENSE"])))

        df = hit_n_run.join(Units_use.select("CRASH_ID", "UNIT_NBR", "VEH_HNR_FL"), on=["CRASH_ID", "UNIT_NBR"])\
            .filter(col("VEH_HNR_FL") == "Y")
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol4}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol4}", index=False)
        
        return df.count()

    @handle_error
    def get_state_with_no_female_accident(Primary_Person_use):
        """
        Finds the state with the highest number of accidents without female involvement.
        
        Parameters:
        - Primary_Person_use: DataFrame containing primary person data.
        
        Returns:
        - str: The state with the highest number of accidents without female involvement.(Ans= Texas- 38442)
        """
        CRASH_ID_FEMALE_INVOLVED = Primary_Person_use\
        .filter(col("PRSN_GNDR_ID") == "FEMALE")\
        .select(collect_list("CRASH_ID").alias("CRASH_ID_LIST"))\
        .first()["CRASH_ID_LIST"]

        df = Primary_Person_use.filter(~(col("CRASH_ID").isin(CRASH_ID_FEMALE_INVOLVED)))\
            .groupBy("DRVR_LIC_STATE_ID")\
            .agg(countDistinct("CRASH_ID").alias("NO_OF_CRASH_WITHOUT_FEMALE"))\
            .orderBy(desc("NO_OF_CRASH_WITHOUT_FEMALE"))
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol5}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol5}", index=False)
        
        return df.first()["DRVR_LIC_STATE_ID"]

    @handle_error
    def get_top_vehicle_contributing_to_injuries(Units_use):
        """
        Finds the VEH_MAKE_IDs ranking from the 3rd to the 5th positions that contribute to the largest number of injuries, including death.
        
        Parameters:
        - Units_use: DataFrame containing units data.
        
        Returns:
        - DataFrame: The VEH_MAKE_IDs ranking from the 3rd to the 5th positions that contribute to the largest number of injuries, including death.(Ans= TOYOTA, DODGE, NISSAN)
        """
        window_spec = Window.orderBy(desc("total_injury_with_death"))

        df = Units_use.withColumn("total_injury_with_death", col("TOT_INJRY_CNT") + col("DEATH_CNT"))\
            .groupBy("VEH_MAKE_ID")\
            .agg(sum(col("total_injury_with_death")).alias("total_injury_with_death"))\
            .withColumn("rnk", row_number().over(window_spec))\
            .filter((col("rnk") >= 3) & (col("rnk") <= 5))
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol6}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol6}", index=False)
        
        return df

    @handle_error
    def get_top_ethnic_ug_crash_for_each_body_style(Primary_Person_use, Units_use):
        """
        Finds and displays the top ethnic user group for each unique body style involved in crashes.
        
        Parameters:
        - Primary_Person_use: DataFrame containing primary person data.
        - Units_use: DataFrame containing units data.
        
        Returns:
        - DataFrame: The top ethnic user group for each unique body style involved in crashes.(Ans=DataFrame)
        """
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("COUNT_of_ETHNICITY"))

        df = Primary_Person_use.join(Units_use, on=["CRASH_ID", "UNIT_NBR"])\
            .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")\
            .agg(count("CRASH_ID").alias("COUNT_of_ETHNICITY"))\
            .withColumn("rnk", dense_rank().over(window_spec))\
            .filter(col("rnk") == 1)
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol7}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol7}", index=False)
        
        return df

    @handle_error
    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(Primary_Person_use, Units_use):
        """
        Finds the top 5 Zip Codes with the highest number of crashes where alcohol is a contributing factor.
        
        Parameters:
        - Primary_Person_use: DataFrame containing primary person data.
        - Units_use: DataFrame containing units data.
        
        Returns:
        - DataFrame: The top 5 Zip Codes with the highest number of alcohol-related crashes.(Ans=DataFrame)
        """
        df = Primary_Person_use.join(Units_use, on=["CRASH_ID", "UNIT_NBR"])\
            .where((col("VEH_BODY_STYL_ID").isin('PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR')) &
                   (col("PRSN_ALC_RSLT_ID") == 'Positive'))\
            .groupBy("DRVR_ZIP").agg(count("DRVR_ZIP").alias("count_zip"))\
            .orderBy(col("count_zip").desc()).limit(5)
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(f"{output_path}{sol8}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol8}", index=False)
        
        return df


    @handle_error
    def get_crash_ids_with_no_damage(Units_use):
        """
        Counts distinct Crash IDs where no damaged property was observed, the damage level (VEH_DMAG_SCL) is above 4,
        and the car has insurance.
        Parameters:
        - Units_use: DataFrame containing units data.

        Returns:
        -The count of distinct Crash IDs meeting the specified criteria.(Ans= 1934)
        """
        df = Units_use.where(((col("VEH_DMAG_SCL_1_ID") == "NO DAMAGE") &\
        (col("VEH_DMAG_SCL_2_ID") == 'NO DAMAGE')) |\
        ((regexp_extract("VEH_DMAG_SCL_1_ID", '\d+', 0) > 4)\
        & (regexp_extract("VEH_DMAG_SCL_2_ID", '\d+', 0) > 4))\
        & col("FIN_RESP_TYPE_ID").contains("INSURANCE")\
        ).select("CRASH_ID").distinct()
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option(\
#       "header", "true").save(f"{output_path}{sol9}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol9}", index=False)

        return df.count()

    @handle_error
    def get_top_5_vehicle_brand(charges_df, person_df, units_df):
        """
        Determines the top 5 Vehicle Makes/Brands where drivers are charged with speeding-related offences,
        have licensed drivers, use the top 10 used vehicle colours, and have cars licensed with the top 25 states
        with the highest number of offences. Parameters: - output_format (str): The file format for writing the
        output. - output_path (str): The file path for the output file. Returns: - List[str]: The list of top 5
        Vehicle Makes/Brands meeting the specified criteria.
        
        Parameters:
        - charges_df: DataFrame containing charges data.
        - Primary_Person_use: DataFrame containing primary person data.
        - Units_use: DataFrame containing units data.
        
        Returns:
        - DataFrame: top 5 Vehicle Makes/Brands meeting the specified criteria.(Ans= DataFrame)
        """
       
        # charges with speed related offences with driver licences
        speeding_with_licences = charges_df.join(person_df, on = ["CRASH_ID","UNIT_NBR"])\
            .where((col("CHARGE").like("%SPEED%")) &
                   (col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")) \
            .select(charges_df.CRASH_ID,
                    charges_df.CHARGE,
                    charges_df.UNIT_NBR,
                    person_df.DRVR_LIC_TYPE_ID,
                    person_df.DRVR_LIC_STATE_ID,
                    person_df.DRVR_LIC_CLS_ID
                    )

        # Top states with highest number of offences
        top_states_offences = person_df\
            .groupBy("DRVR_LIC_STATE_ID")\
            .count()\
            .orderBy(col("count").desc()).take(25)

        # Top used vehicles colours with licenced
        top_licensed_vehicle_colors = units_df.\
            join(person_df, on = ["CRASH_ID","UNIT_NBR"]) \
            .where((col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")
                   & (col("DRVR_LIC_CLS_ID").like("CLASS C%"))) \
            .groupBy("VEH_COLOR_ID")\
            .count()\
            .orderBy(col("count").desc()).take(10)

        top_colors = [i["VEH_COLOR_ID"] for i in top_licensed_vehicle_colors]
        top_states = [i['DRVR_LIC_STATE_ID'] for i in top_states_offences]

        df = speeding_with_licences\
            .join(units_df, on = ["CRASH_ID","UNIT_NBR"]) \
            .where((col("VEH_COLOR_ID").isin(top_colors))
                   & (col("DRVR_LIC_STATE_ID").isin(top_states))) \
            .select("VEH_MAKE_ID")
        
#       df.coalesce(1).write.format("csv").mode("overwrite").option(\
#       "header", "true").save(f"{output_path}{sol10}")
        df = df.toPandas()
        df.to_csv(f"{output_path}{sol10}", index=False)        

        return df