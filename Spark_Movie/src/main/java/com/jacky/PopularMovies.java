package com.jacky;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class PopularMovies {
    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession.builder()
                .config("spark.driver.host", "localhost")
                .config("spark.sql.catalogImplementation","hive")
                .appName("Popular Movies")
                .master("local").getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");


    }
}
