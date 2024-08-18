import yaml
from pyspark.sql import SparkSession
from error_handling import handle_error

@handle_error
def read_data(spark):
    """
    Reads the required raw tables from input folder.

    Args:
        spark: SparkSession object used to read data.

    Returns:
        Tuple of DataFrames containing the raw tables.
    """
    
    # Load configuration
    try:
        with open("C:/Users/user/Downloads/src/CrashAnalysisProject/src/config.yml", 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        print(f"Error loading config file: {str(e)}")
        raise

    # Extract paths from config
    try:
        inputpath = config["files"]["inputpath"]
        charges = config["files"]["charges"]
        damages = config["files"]["damages"]
        endorse = config["files"]["endorse"]
        person = config["files"]["person"]
        restrict = config["files"]["restrict"]
        units = config["files"]["units"]
    except KeyError as e:
        print(f"Missing key in config file: {str(e)}")
        raise

    # Read data
    try:
        charges_df = spark.read.format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(inputpath + charges)

        damages_df = spark.read.format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(inputpath + damages)

        endorse_df = spark.read.format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(inputpath + endorse)
        
        person_df = spark.read.format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(inputpath + person)
        
        restrict_df = spark.read.format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(inputpath + restrict)
        
        units_df = spark.read.format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(inputpath + units)
        
    except Exception as e:
        print(f"Error reading data: {str(e)}")
        raise

    return (
        charges_df,
        damages_df,
        endorse_df,
        person_df,
        restrict_df,
        units_df
    )
