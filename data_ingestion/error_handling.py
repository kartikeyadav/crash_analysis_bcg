import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

logging.basicConfig(level=logging.ERROR)

def handle_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in function {func.__name__}: {str(e)}")
            print(f"Error in main function: {str(e)}")
            raise SystemExit(f"Exiting due to the error: {str(e)}")
    return wrapper
