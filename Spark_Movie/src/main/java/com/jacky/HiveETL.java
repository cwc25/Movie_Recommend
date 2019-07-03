package com.jacky;

import com.jacky.Model.Movie;
import com.jacky.Model.Rating;
import com.jacky.Util.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import sun.jvm.hotspot.asm.sparc.SPARCArgument;


import java.io.Console;
import java.util.List;

public class HiveETL {
    public static void main(String[] args) throws Exception
    {
        SparkSession spark = SparkSession.builder()
                .config("spark.driver.host", "localhost")
                .config("spark.sql.catalogImplementation","hive")
                .appName("Movie Recommend Hive ETL")
                .master("local").getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");

        //spark.sql("drop database if exists jacky cascade");
        //Job 1 - 1. incremental update setup 2. load rating and movie info into hive and prepare training and test data
///***
        //-----database creation
        spark.sql("CREATE DATABASE IF NOT EXISTS jacky");
        spark.catalog().listDatabases().show();
        spark.catalog().setCurrentDatabase("jacky");

        //hive records remain in table when run the same main class


        //
        //
        //Movie incremental update implementation
        //
        //

        JavaRDD<Movie> movies = sc.textFile("Spark_Movie/data1/movies.dat").map(record -> {
            String[] movieSplit = record.split(",");
            return new Movie(movieSplit[0], movieSplit[1], movieSplit[2]);
        });

        Dataset<Row> movieDf = spark.createDataFrame(movies, Movie.class);
        movieDf.createOrReplaceTempView("movieTemp");

        //movie_base table creation
        spark.sql("create table if not exists movies_base(movieId int, title string, genres string) stored as parquet");
        long baseMovieCount = spark.sql("select count(*) from movies_base").first().getLong(0);

        if(baseMovieCount == 0){ //first time load, insert into base table
            spark.sql("insert into table movies_base select movieId, title, genres from movieTemp");
            System.out.println("initial load -- movies_base:");
            spark.sql("select * from movies_base").show(10);
        }
        else {//incremental update, insert to incremental external table (external table must have location, and we use rdd to load hdfs files so can't use external table here)
            spark.sql("create table if not exists movies_incremental (movieId int, title string, genres string) stored as parquet");
            spark.sql("insert overwrite table movies_incremental select movieId, title, genres from movieTemp");
            System.out.println("incremental load -- movies_incremental:");
            spark.sql("select * from movies_incremental").show(10);
        }

        System.out.println("base movie count: " + baseMovieCount);
        if(baseMovieCount ==0){
            spark.sql("create view if not exists movies_reconcile_view(movieId, title, genres) as " +
                    "select * from movies_base");
        }
        else {
            //create movie update-to-date view
            System.out.println("union view:");
            spark.sql("select * from movies_base union select * from movies_incremental").show();

            spark.sql("create view if not exists movies_reconcile_view(movieId, title, genres) as " +
                    "(select * from movies_base union select * from movies_incremental)");
        }

        System.out.println("movies_reconcile_view:");
        spark.sql("select * from movies_reconcile_view").show(10);

        spark.sql("create table if not exists movies_reporting(movieId int, title string, genres string) stored as parquet");
        spark.sql("insert overwrite table movies_reporting select * from movies_reconcile_view");
        spark.sql("drop view if exists movies_reconcile_view");

        //movie_base table up to date
        spark.sql("insert overwrite table movies_base select * from movies_reporting");

        System.out.println("movies_reporting:");
        spark.sql("select * from movies_reporting").show(10);

        System.out.println("cycle end -- movies_base:");
        spark.sql("select * from movies_base").show(10);

        //archive movie records into external table
        spark.sql("create external table if not exists movies_reporting_archive(movieId int, title string, genres string) location 'Spark_Movie/data1/archive/movie/'");

        if(baseMovieCount ==0) {
            spark.sql("insert into table movies_reporting_archive select * from movies_base");
        }
        else{
            spark.sql("insert into table movies_reporting_archive select * from movies_incremental");
        }

        System.out.println("movies archive: ");
        spark.sql("select * from movies_reporting_archive").show(10);

         //
         //
         //
         // Rating incremental update implementation
         //
         //

        JavaRDD<Rating> ratings = sc.textFile("Spark_Movie/data1/ratings.dat").map(record -> {
            String[] ratingSplit = record.split(",");
            return new Rating(ratingSplit[0], ratingSplit[1], ratingSplit[2], ratingSplit[3]);
        }).filter(rating -> { return rating.getTimes() != null;}); //data clearning - filter records with times column null value

        Dataset<Row> ratingDf = spark.createDataFrame(ratings, Rating.class);
        ratingDf.createOrReplaceTempView("ratingTemp");

        //rating_base table creation
        spark.sql("create table if not exists ratings_base(userId String, movieId String, rating String, times String) stored as parquet");
        long baseRatingCount = spark.sql("select count(*) from ratings_base").first().getLong(0);

        if(baseRatingCount == 0){ //first time load, insert into base table
            spark.sql("insert into table ratings_base select userId, movieId, rating, times from ratingTemp");
            System.out.println("initial load -- rating_base:");
            spark.sql("select * from ratings_base").show(10);
        }
        else {//incremental update, insert to incremental external table (external table must have location, and we use rdd to load hdfs files so can't use external table here)
            spark.sql("create table if not exists ratings_incremental (userId String, movieId String, rating String, times String) stored as parquet");
            spark.sql("insert overwrite table ratings_incremental select userId, movieId, rating, times from ratingTemp");
            System.out.println("incremental load -- ratings_incremental:");
            spark.sql("select * from ratings_incremental").show(10);
        }

        System.out.println("base rating count: " + baseRatingCount);
        if(baseRatingCount ==0){
            spark.sql("create view if not exists ratings_reconcile_view(userId, movieId, rating, times) as " +
                    "select * from ratings_base");
        }
        else {
            //create movie update-to-date view
            System.out.println("union view:");
            spark.sql("select * from ratings_base union select * from ratings_incremental").show();

            spark.sql("create view if not exists ratings_reconcile_view(userId, movieId, rating, times) as " +
                    "(select * from ratings_base union select * from ratings_incremental)");
        }

        System.out.println("movies_reconcile_view:");
        spark.sql("select * from ratings_reconcile_view").show(10);

        spark.sql("create table if not exists ratings_reporting(userId String, movieId String, rating String, times String) stored as parquet");
        spark.sql("insert overwrite table ratings_reporting select * from ratings_reconcile_view");
        spark.sql("drop view if exists ratings_reconcile_view");

        //movie_base table up to date
        spark.sql("insert overwrite table ratings_base select * from ratings_reporting");

        System.out.println("ratings_reporting:");
        spark.sql("select * from ratings_reporting").show(10);

        System.out.println("cycle end -- ratings_base:");
        spark.sql("select * from ratings_base").show(10);

        //split rating data for training and test data set
        long ratingCount = spark.sql("select count(*) from ratings_reporting").first().getLong(0);
        System.out.println("total training count: " + ratingCount);
        double percent = 0.6;
        int trainingDataCount = (int)(ratingCount * percent);
        int testDataCount = (int)(ratingCount * (1 - percent));

        System.out.println("training data count: " + trainingDataCount);
        System.out.println("test data count: " + testDataCount);

        Dataset<Row> trainingDataAscDf = spark.sql("select userId, movieId, rating, times from ratings_reporting order by times asc limit " + trainingDataCount);
        Dataset<Row> trainingDataDescDf = spark.sql("select userId, movieId, rating, times from ratings_reporting order by times desc limit " + testDataCount);

        trainingDataAscDf.createOrReplaceTempView("trainingDataAsc");
        trainingDataDescDf.createOrReplaceTempView("trainingDataDesc");

        System.out.println("trainingDataAsc view: ");
        spark.sql("select * from trainingDataAsc").show(10);

        spark.sql("create table if not exists trainingData (userId string, movieId string, rating string, times string) stored as parquet");
        spark.sql("create table if not exists testData (userId string, movieId string, rating string, times string) stored as parquet");

        //full insert
        spark.sql("insert overwrite table trainingData select userId, movieId, rating, times from trainingDataAsc");
        spark.sql("insert overwrite table testData select userId, movieId, rating, times from trainingDataDesc");

        System.out.println("training data asc: ");
        spark.sql("select * from trainingData").show(10);
        System.out.println("training data desc: ");
        spark.sql("select * from testData").show(10);

        spark.sql("create external table if not exists  ratings_reporting_archive(userId string, movieId string, rating string, times string) location 'Spark_Movie/data1/archive/rating/'");

        if(baseRatingCount == 0) {
            spark.sql("insert into table ratings_reporting_archive select * from ratings_base");
        }
        else{
            spark.sql("insert into table ratings_reporting_archive select * from ratings_incremental");
        }

        System.out.println("ratings archive: ");
        spark.sql("select * from ratings_reporting_archive").show(10);
//**/
        //Job 2 - Get top 5 movies for first time visit users





    }
}
