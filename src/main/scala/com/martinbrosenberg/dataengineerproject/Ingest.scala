package com.martinbrosenberg.dataengineerproject

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Ingest {

  case class Table(name: String, data: DataFrame)

  def buildSparkSession(): SparkSession = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local") // Don't do this in production
      .getOrCreate()

  def ingestKeywords(spark: SparkSession, tables: Seq[Table]): Seq[Table] = {
    import spark.implicits._

    val keywordsSchema = ArrayType(
      StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))
    )

    val raw = spark.read
      .option("header", "true")
      .csv("./the-movies-dataset/keywords.csv")
      .select(
        $"id",
        explode(from_json($"keywords", keywordsSchema)).as("keywords")
      )

    val keywords = raw
      .select($"keywords.*")
      .distinct()
      .sort($"id")

    val map = raw
      .select(
        $"id".cast(IntegerType).as("movie_id"),
        $"keywords.id".as("keyword_id")
      )
      .sort($"movie_id", $"keyword_id")
      .select(monotonically_increasing_id().as("id"), $"*")

    tables :+ Table("keywords", keywords) :+ Table("movies_keywords_map", map)
  }

  def ingestRatings(spark: SparkSession, tables: Seq[Table]): Seq[Table] = {
    import spark.implicits._

    val ratings = spark.read
      .option("header", "true")
      .csv("./the-movies-dataset/ratings_small.csv")
      .sort($"userId", $"movieId")
      .select(
        monotonically_increasing_id().as("id"),
        $"userId",
        $"movieId",
        $"rating".as("user_movie_rating"),
        from_unixtime($"timestamp").as("timestamp")
      )

    tables :+ Table("ratings", ratings)
  }

  def ingestMovies(spark: SparkSession, tables: Seq[Table]): Seq[Table] = {
    import spark.implicits._

    val raw = spark.read
      .option("header", "true")
      .csv("./the-movies-dataset/movies_metadata.csv")

    val collectionSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    val collections = raw
      .select(
        from_json(
          regexp_replace($"belongs_to_collection", ": None", ": null"),
          collectionSchema
        ).as("collection")
      )
      .select($"collection.*")
      .sort($"id")
      .filter($"collection".isNotNull)
      .distinct()

    val genresSchema = ArrayType(
      StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))
    )

    val genres = raw
      .select(explode(from_json($"genres", genresSchema)).as("genres"))
      .select($"genres.*")
      .distinct()
      .sort($"id")

    val productionCompaniesSchema = ArrayType(
      StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))
    )

    val productionCompanies = raw
      .select(explode(from_json($"production_companies", productionCompaniesSchema)).as("production_companies"))
      .select($"production_companies.*")
      .distinct()
      .sort($"id")

    val productionCountriesSchema = ArrayType(
      StructType(Array(
        StructField("iso_3166_1", StringType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))
    )

    val productionCountries = raw
      .select(explode(from_json($"production_countries", productionCountriesSchema)).as("production_countries"))
      .select($"production_countries.*")
      .distinct()
      .sort($"iso_3166_1")

    val spokenLanguagesSchema = ArrayType(
      StructType(Array(
        StructField("iso_639_1", StringType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))
    )

    val languages = raw
      .select(explode(from_json($"spoken_languages", spokenLanguagesSchema)).as("languages"))
      .select($"languages.*")
      .distinct()
      .sort($"iso_639_1")

    val movies = raw
      .select(
        $"adult".cast(BooleanType).as("adult"),
        when($"budget" === "0", null).otherwise($"budget").cast(IntegerType).as("budget"),
        $"homepage".as("website"),
        $"id".cast(IntegerType).as("tmdb_id"),
        $"imdb_id",
        // original language => refer to spoken_languages
        $"original_title",
        $"overview",
        $"popularity".cast(DoubleType).as("popularity"),
        $"release_date".cast(DateType).as("releaseDate"),
        $"revenue".cast(IntegerType).as("revenue"),
        $"runtime".cast(IntegerType).as("runtime"),
        $"status",
        $"tagline",
        $"title",
        $"vote_average".cast(DoubleType).as("vote_average"),
        $"vote_count".cast(IntegerType).as("vote_count")
      )

    tables :+ Table("movies", movies)

  }

  def main(args: Array[String]) {
    val spark = buildSparkSession()

    var tables = Seq.empty[Table]
    tables = ingestKeywords(spark, tables)
    tables = ingestRatings(spark, tables)

    spark.stop()
  }
}