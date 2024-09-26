from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("MovieLensAnalysis") \
    .getOrCreate()

# Cargar los datos de MovieLens (asegúrate de que la ruta sea correcta)
ratings = spark.read.csv("/tmp/app/ratings.csv", header=True, inferSchema=True)
movies = spark.read.csv("/tmp/app/movies.csv", header=True, inferSchema=True)

# Calcular el promedio de calificaciones por película
average_ratings = ratings.groupBy("movieId").agg(avg("rating").alias("avg_rating"))

# Unir con el dataset de películas
movie_avg_ratings = average_ratings.join(movies, on="movieId", how="inner")

# Mostrar los resultados de las películas mejor valoradas
movie_avg_ratings.orderBy(col("avg_rating").desc()).show(10)

# Guardar los resultados en un archivo CSV
movie_avg_ratings.write.csv("/tmp/app/output/movie_ratings")

# Detener la sesión de Spark
spark.stop()
