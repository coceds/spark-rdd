package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class Application3 {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("Spark test")
                        .setMaster("local")
        );

        JavaPairRDD<String, String> countries = sc.textFile("hdfs://192.168.56.101:8020/user/cloudera/countries/").
                mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] parts = s.split(",");
                        if (parts.length >= 5) {
                            return new Tuple2<>(parts[0], parts[5]);
                        } else {
                            return new Tuple2<>(parts[0], "Not identified");
                        }
                    }
                });
        final Broadcast<Map<String, String>> broadcastCountries = sc.broadcast(countries.collectAsMap());

        JavaRDD<String> records = sc.textFile(
                "hdfs://192.168.56.101:8020/user/cloudera/flume/events2/2018/02/07/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events2/2018/02/08/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events2/2018/02/09/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events2/2018/02/10/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events2/2018/02/11/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events2/2018/02/12/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events2/2018/02/13/");
        CodeToIp local = CodeToIp.init();
        final Broadcast<CodeToIp> broadcast = sc.broadcast(local);

        JavaPairRDD<String, Double> pairRDD = records
                .map(new Function<String, String[]>() {
                    @Override
                    public String[] call(String v1) throws Exception {
                        return v1.split(",");
                    }
                })
                .mapToPair(new PairFunction<String[], String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String[] strings) throws Exception {
                        String geoId = broadcast.value().getGeoId(strings[5]);
                        Double price = Double.valueOf(strings[3]);
                        return new Tuple2<>(geoId, price);
                    }
                });

        JavaPairRDD<String, Double> byCountry = pairRDD.reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });


        JavaPairRDD<Double, Tuple2<String, String>> reverted = byCountry
                .mapToPair(new PairFunction<Tuple2<String, Double>, Double, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<Double, Tuple2<String, String>> call(Tuple2<String, Double> t) throws Exception {
                        String name = broadcastCountries.getValue().get(t._1);
                        return new Tuple2<>(t._2, new Tuple2<>(name, t._1));
                    }
                }).sortByKey(false);


        List<Tuple2<Double, Tuple2<String, String>>> result = reverted.take(10);

        for (Tuple2<Double, Tuple2<String, String>> t : result) {
            System.out.println(t._1 + " " + t._2._1);
        }
    }

}
