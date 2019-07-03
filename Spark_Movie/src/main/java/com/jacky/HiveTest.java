package com.jacky;

import org.apache.spark.sql.SparkSession;

public class HiveTest {
    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession.builder()
                .config("spark.driver.host", "localhost")
                .appName("Hive Test")
                .master("local").getOrCreate();

        //spark.catalog().setCurrentDatabase("jacky");
        spark.catalog().listTables().show();
    }
}
