from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp
from utils import utils
import os

class ingestable:
    def __init__(self, source, path, file_format):
        self.source = source
        self.path = path
        self.file_format = file_format
        self.output_format = "parquet"
        self.output_mode = "append"
        self.__checkpoint_path = os.path.join(os.getcwd(), "..", "checkpoints", f"{self.source}")
        self.landing_directory = os.path.join(os.getcwd(), "..", "landing", f"{self.source}")
    
    def get_source(self):
        return self.source
    
    def get_path(self):
        return self.path
    
    def get_file_format(self):
        return self.file_format
    
    def get_landing_directory(self):
        return self.landing_directory
    
    def __repr__(self):
        return f"ingestable({self.source},{self.path},{self.file_format})"
    
    def ingest(self, spark_session):
        schema = StructType() \
            .add("transaction_id", IntegerType()) \
            .add("transaction_date", StringType()) \
            .add("amount", DoubleType()) \
            .add("transaction_type", StringType()) \
            .add("merchant_name", StringType()) \
            .add("category", StringType()) \
            .add("account_type", StringType()) \
            .add("location", StringType()) \
            .add("currency", StringType()) \
            .add("description", StringType())  

        # 🔹 Leer datos en modo streaming
        df_stream = (
            spark_session.readStream
            .format(self.file_format)
            .option("header", "true")
            .schema(schema)
            .load(self.path)
            .withColumn("processing_time", current_timestamp())  # 🔹 Añadir timestamp de procesado
        )

        query = (
            df_stream.writeStream
            .format(self.output_format) 
            .outputMode(self.output_mode)
            .option("checkpointLocation", self.__checkpoint_path)
            .option("path", self.landing_directory)  # 📌 Agregar ruta de salida
            .trigger(once=True)
            .start()
        )

        query.awaitTermination()

        if query.lastProgress:
            print(f"Processed rows: {query.lastProgress['numInputRows']}")
        else:
            print("No rows were processed.")
        
    def clear_checkpoint(self):
        try:
            utils.dir_utils.recursive_deletion(self.__checkpoint_path)
            utils.dir_utils.recursive_deletion(self.landing_directory)
        except OSError as error:
            raise error