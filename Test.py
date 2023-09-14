from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

columns = ["Продукт", "Категории"]

data = [("Продукт1", ["Категория1", "Категория2"]),
        ("Продукт2", ["Категория2"]),
        ("Продукт3", ["Категория1", "Категория3"]),
        ("Продукт4", [])]

df = spark.createDataFrame(data, columns)

product_category_df = df.select(col("Продукт"), explode(col("Категории")).alias("Категория"))

products_without_category_df = df.filter(col("Категории").isNull()).select(col("Продукт"), col("Категории"))

result_df = product_category_df.union(products_without_category_df)

result_df.show()