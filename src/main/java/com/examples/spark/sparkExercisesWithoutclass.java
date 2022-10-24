package com.examples.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.*;

public class sparkExercisesWithoutclass {

    void process() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        long start = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder().appName("GroupByHashing").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        String[][] input = { {"50000.0#0#0#", "#"}, {"0@1000.0@", "@"}, {"1$", "$"}, {"1000.00^Test_string", "^"} };

        ArrayList<Result> resultList = new ArrayList<>();

        for(String[] stingarray : input)
            resultList.add(new Result(stingarray[0],stingarray[1]));

        Dataset<Result> tupleDS = spark.createDataset(resultList, Encoders.bean(Result.class));

//        tupleDS.printSchema();
        tupleDS.show();

        Dataset<Row> replacedDF = tupleDS.toDF()
                .select(col("values"), col("delimiter"))
                .withColumn("replacedValues", regexp_replace(col("values"), col("delimiter"), lit("del")));

        replacedDF.show();

        replacedDF.withColumn("split_values",split(col("replacedValues"), "del"))
            .select(col("values"), col("delimiter"), col("split_values"))
            .show(false);
    }

    public static void main(String[] args) {
        sparkExercisesWithoutclass sparkExercises = new sparkExercisesWithoutclass();
        sparkExercises.process();
    }
}
