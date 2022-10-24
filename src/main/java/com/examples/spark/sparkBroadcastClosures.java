package com.examples.spark;

//import org.apache.log4j.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2;

import java.util.List;
import java.util.Scanner;

public class sparkBroadcastClosures {
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
//        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        //action
        Dataset<Row> ds = spark.read().option("header","true").csv("src/main/resources/products.csv");
        ds.show();

        Dataset<Tuple2<String, String>> prCode = ds.map((Function1<Row, Tuple2<String, String>>) row ->
                new Tuple2<String, String>(row.getAs(0), row.getAs(1)),
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()));



        prCode.show();

//        System.out.println(prCode.size());


        Scanner sc = new Scanner(System.in);
        sc.next();
    }
}
