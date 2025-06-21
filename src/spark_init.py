from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession

_spark_session = None

def get_spark_session() -> SparkSession:
    global _spark_session
    if _spark_session == None:
        
        try:
            _spark_session = SparkSession.builder.getOrCreate()
        except:
            try:
                _spark_session = DatabricksSession.builder.getOrCreate()
            except Exception as error:
                raise RuntimeError("Failed to initialize spark session.") from error
    return _spark_session

