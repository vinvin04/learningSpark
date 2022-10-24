package com.examples.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class GroupByHashing {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        long start = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder().appName("GroupByHashing").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> listDS = spark.read().option("header", "true").csv("C:\\datasets\\flipkart_com-ecommerce_sample.csv");
        listDS.printSchema();
        listDS.show();

        //uniq_id,crawl_timestamp,product_url,product_name,product_category_tree,pid,retail_price,discounted_price,image,
        // is_FK_Advantage_product,description,product_rating,overall_rating,brand,product_specifications

        Column[] groupbyColsArray = new Column[]{col("product_url"), col("product_name"), col("product_category_tree"),
                col("pid"), col("retail_price"), col("discounted_price"),
                col("is_FK_Advantage_product"), col("description"), col("product_rating"), col("overall_rating"),
                col("brand"), col("product_specifications")};

//        Column[] groupByuniqid = new Column[] { col("uniq_id")};

        Dataset<Row> uniqueIdDS = listDS.withColumn("new_unique_id", monotonically_increasing_id());

        Dataset<Row> groupByDS = uniqueIdDS.groupBy("new_unique_id")
                .count();
        List<Row> list = groupByDS.collectAsList();

        System.out.println("size of list " + list.size());
        long end = System.currentTimeMillis();

        System.out.println("time "+ TimeUnit.MILLISECONDS.toSeconds(end - start)+"sec");
//        Scanner scanner = new Scanner(System.in);
//        scanner.next();
    }
}
