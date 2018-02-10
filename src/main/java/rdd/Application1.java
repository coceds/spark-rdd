package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Application1 {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("Spark test")
//                        .setMaster("local")
        );

        JavaRDD<String> records = sc.textFile(
                "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/06/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/07/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/08/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/09/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/10/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/11/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/12/");

        JavaPairRDD<String, Integer> pairRDD = records
                .map(new Function<String, String[]>() {
                    @Override
                    public String[] call(String v1) throws Exception {
                        return v1.split(",");
                    }
                })
                .mapToPair(new PairFunction<String[], String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String[] strings) throws Exception {
                        return new Tuple2<>(strings[4], 1);
                    }
                });

        JavaPairRDD<String, Integer> byType = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<Integer, String>> result = byType
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                        return new Tuple2<Integer, String>(t._2, t._1);
                    }
                })
                .sortByKey(false)
                .take(10);
        for (Tuple2<Integer, String> tuple2 : result) {
            System.out.println(tuple2);
        }
    }

}
