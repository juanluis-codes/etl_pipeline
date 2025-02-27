from pyspark.sql import SparkSession
from threading import Lock

class SingletonSparkSession:
    _instance = None
    _lock = Lock()

    def __new__(cls, app_name="SingletonSparkApp"):
        with cls._lock:  # Asegura seguridad en entornos multi-hilo
            if cls._instance is None:
                cls._instance = super(SingletonSparkSession, cls).__new__(cls)
                cls._instance.spark = SparkSession.builder.appName(app_name).getOrCreate()
        return cls._instance

    def get_spark(self):
        return self.spark
