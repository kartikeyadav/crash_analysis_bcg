
from pyspark.sql import SparkSession
import yaml
import os

with open("C:/Users/user/Downloads/src/CrashAnalysisProject/src/config.yml", 'r') as file:
     config = yaml.safe_load(file)
        
java_home = config["java_home"]      
        

os.environ["JAVA_HOME"] = java_home
os.environ["PATH"] = java_home + r"\bin;" + os.environ["PATH"]

class SparkSessionSingleton:
    _spark = None

    @staticmethod
    def get_spark_session():
        if SparkSessionSingleton._spark is None:
            SparkSessionSingleton()
        return SparkSessionSingleton._spark

    def __init__(self):
        if SparkSessionSingleton._spark is not None:
            raise Exception("SparkSessionSingleton already instantiated!")
        SparkSessionSingleton._spark = SparkSession.builder.appName("CarCrash").getOrCreate()

