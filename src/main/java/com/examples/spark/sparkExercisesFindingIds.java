package com.examples.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;


public class sparkExercisesFindingIds {



    void process() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        long start = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder().appName("GroupByHashing").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        //https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html
        //data creation
        StructType structType = new StructType();
        structType = structType.add("id", DataTypes.StringType, false);
        structType = structType.add("words", DataTypes.StringType, false);
        structType = structType.add("word", DataTypes.StringType, false);

        List<Row> rows = new ArrayList<Row>();

        rows.add(RowFactory.create("1", "one,two,three", "one"));
        rows.add(RowFactory.create("2","four,one,five", "six"));
        rows.add(RowFactory.create("3", "seven,nine,one,two", "eight"));
        rows.add(RowFactory.create("4", "two,three,five", "five"));
        rows.add(RowFactory.create("5", "six,five,one", "seven"));

        //process start
        Dataset<Row> df = spark.createDataFrame(rows, structType)
                .withColumn("split", split(col("words"),","))
                .withColumn("split",explode(col("split")));

        Dataset<Row> allowedWordsdf = df.select(col("word")).distinct();

        df.groupBy(col("split")).agg(collect_list(col("id")))
            .join(allowedWordsdf,df.col("split").equalTo(allowedWordsdf.col("word")))
                .select(col("split").as("word"),col("collect_list(id)").as("ids")).show();

        new Scanner(System.in).next();
    }

    public static void main(String[] args) {
        sparkExercisesFindingIds sparkExercises = new sparkExercisesFindingIds();
        sparkExercises.process();
    }

}
