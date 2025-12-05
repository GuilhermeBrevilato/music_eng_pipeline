from pyspark.sql import SparkSession

def get_spark(app_name="MusicEngPipeline"):
    """
    Cria ou reutiliza uma SparkSession padronizada para o projeto.
    """
    spark = (
        SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.driver.memory", "4g")
            .getOrCreate()
    )

    return spark
