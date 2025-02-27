from pyspark.sql.functions import col, to_date, expr
import os

class transformable:
    def __init__(self, source, source_df, layer, spark=None):
        self.source_df = source_df
        self.target_df = None
        self.spark = spark
        self.layer = layer
        self.target_dir = os.path.join(os.getcwd(), "..", layer, f"{source}")
        
    def get_target_directory(self):
        return self.target_dir
        
    def to_bronze(self):
        # bronze is a direct copy of landing
        self.target_df = self.source_df
        self.target_df.write.mode("append").parquet(self.target_dir)
    
    def to_silver(self):
        transformed_df = (
            self.source_df
            .withColumn("transaction_date", to_date(col("transaction_date"), "M/d/yyyy"))
            .drop("transaction_id")
        )
        
        transformed_df = (
            transformed_df.na.drop(subset=["transaction_date", "amount", "merchant_name", "currency"])
        )
        
        self.target_df = (
            transformed_df.dropDuplicates()
        )
        
        target_df_read = self.spark.read.parquet(self.target_dir)
        
        #self.target_df = (
            #self.target_df.merge(target_df_read, on=)
        #)
        
        self.target_df.write.mode("append").parquet(self.target_dir)
    
    def to_gold(self):
        pass
    
    def run(self):
        if self.layer == "bronze":
            self.to_bronze()
        elif self.layer == "silver":
            self.to_silver()
        elif self.layer == "gold":
            self.to_gold()
        else:
            raise ValueError("Layer value do not match")
        