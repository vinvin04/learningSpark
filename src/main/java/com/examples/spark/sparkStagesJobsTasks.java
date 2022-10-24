package com.examples.spark;

//import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class sparkStagesJobsTasks {
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
//        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        //action
        Dataset<Row> ds = spark.read().option("header","true").csv("src/main/resources/table1.csv");

        Dataset<Row> repartionDs = ds.repartition(2);

        Dataset<Row> countDs = repartionDs.where("col1 != 'a'")
                .select("col1", "col2", "col3")
                .groupBy("col1")
                .count();
        //action
        System.out.println(countDs.collect().toString());

        Scanner sc = new Scanner(System.in);
        sc.next();
    }
}
