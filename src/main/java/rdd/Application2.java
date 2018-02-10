package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.PriorityQueue;

public class Application2 {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("Spark test")
                        //.setMaster("local")
        );

        JavaRDD<String> records = sc.textFile(
                "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/06/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/07/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/08/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/09/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/10/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/11/," +
                        "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/12/");

        JavaPairRDD<Tuple2<String, String>, Long> pairRDD = records
                .map(new Function<String, String[]>() {
                    @Override
                    public String[] call(String v1) throws Exception {
                        return v1.split(",");
                    }
                })
                .mapToPair(new PairFunction<String[], Tuple2<String, String>, Long>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Long> call(String[] arr) throws Exception {
                        return new Tuple2<>(new Tuple2<>(arr[4], arr[2]), 1L);
                    }
                });

        JavaPairRDD<Tuple2<String, String>, Long> byTypeAndProduct = pairRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<String, Tuple2<String, Long>> byType = byTypeAndProduct
                .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Long>, String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, Long>> call(Tuple2<Tuple2<String, String>, Long> s) throws Exception {
                        return new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2));
                    }
                });

        JavaPairRDD<String, PriorityQueue<Container>> result = byType.aggregateByKey(
                new PriorityQueue<Container>(),
                new Function2<PriorityQueue<Container>, Tuple2<String, Long>, PriorityQueue<Container>>() {
                    @Override
                    public PriorityQueue<Container> call(PriorityQueue<Container> v1, Tuple2<String, Long> v2) throws Exception {
                        putIf(v1, v2._2, v2._1);
                        return v1;
                    }
                },
                new Function2<PriorityQueue<Container>, PriorityQueue<Container>, PriorityQueue<Container>>() {
                    @Override
                    public PriorityQueue<Container> call(PriorityQueue<Container> v1, PriorityQueue<Container> v2) throws Exception {
                        for (Container entry : v2) {
                            putIf(v1, entry);
                        }
                        return v1;
                    }
                });

        JavaPairRDD<String, Container> flat = result.flatMapValues(new Function<PriorityQueue<Container>, Iterable<Container>>() {
            @Override
            public Iterable<Container> call(PriorityQueue<Container> v1) throws Exception {
                return v1;
            }
        });
        List<Tuple2<String, Container>> r = flat.collect();

        for (Tuple2<String, Container> tuple2 : r) {
            System.out.println(tuple2);
        }
    }


    private static void putIf(PriorityQueue<Container> queue, Container container) {
        putIf(queue, container.count, container.name);
    }

    private static void putIf(PriorityQueue<Container> queue, Long key, String value) {
        if (queue.size() < 10) {
            queue.add(new Container(key, value));
        } else {
            Container lowest = queue.peek();
            if (key > lowest.count) {
                queue.poll();
                queue.add(new Container(key, value));
            }
        }
    }

    public static class Container implements Comparable<Container>, Serializable {
        private Long count;
        private String name;

        public Container(Long count, String name) {
            this.count = count;
            this.name = name;
        }

        public Container() {
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public int compareTo(Container o) {
            return (int) (this.count - o.count);
        }

        @Override
        public String toString() {
            return name + " " + count;
        }
    }
}
