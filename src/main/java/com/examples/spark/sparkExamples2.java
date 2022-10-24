package com.examples.spark;

//import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class sparkExamples2 {
    public class BarList implements Serializable {
        List<String> list;

        public List<String> getList() {
            return list;
        }
        public void setList(List<String> l) {
            list = l;
        }
    }
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
//        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<String> listDS = spark.read().textFile("src/main/resources/list.csv");
        listDS.printSchema();
        listDS.show();

        Dataset<Integer> noAlpha = listDS.map((MapFunction<String, Integer>) x ->
                Arrays.stream(x.split(","))
                        .filter(y -> !Character.isAlphabetic(y.charAt(0)))
                        .map(s -> Integer.parseInt(s))
                        .reduce((a, b) -> a + b).get(), Encoders.INT());
        //sum of rows
        noAlpha.show();

        //sum of columns



       /* Dataset<Row> splitDS = noAlpha.withColumn("value", split(column("value"), ","));
        splitDS.printSchema();
        splitDS.show(false);*/









//        splitDS.map((MapFunction<Row, Row >) x -> x.getAs(0).stream().filter(y->Character.isAlphabetic(y.charAt(0))),Encoders.bean(BarList.class));


       /* Dataset<Row> noalpaDS = splitDS.map(new Function<Row, Row>() {
            public Row call(Row record) throws Exception {
                List<String>
                 x = record.getAs(0);

                return RowFactory.create(x.stream().filter(y -> Character.isAlphabetic(y.charAt(0))));
            }
        }, Encoders.bean(BarList.class));*/

//        noalpaDS.show();

//        Dataset<String> splitDS = listDS.flatMap((FlatMapFunction<String, String>) x ->
//                Arrays.stream(x.split(",")).iterator(), Encoders.STRING());
//        splitDS.show();
    }
}
