package com.examples.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;


public class sparkExercises {



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

        tupleDS.printSchema();
        tupleDS.show();

        tupleDS = tupleDS.map((MapFunction<Result,Result>) x -> {
            x.setReplacedValues(x.values.replaceAll(x.delimiter,"del"));
            return x;
            }, Encoders.bean(Result.class));

        tupleDS.show();

        Dataset<Row> rowDS = tupleDS.withColumn("split_values", split(col("replacedValues"), "del"));

        rowDS.show();

//        tupleDS = tupleDS.map((MapFunction<Result,Result>) x -> {
//            x.list.addAll(Arrays.asList(x.values.split(x.delimiter)));
//            return x;
//        }, Encoders.bean(Result.class));

//        tupleDS.show();
    }

    public static void main(String[] args) {
        sparkExercises sparkExercises = new sparkExercises();
        sparkExercises.process();
    }

}
