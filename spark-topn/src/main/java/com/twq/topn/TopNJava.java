package com.twq.topn;

import com.google.common.collect.Ordering;
import com.twq.local.BoundedPriorityQueue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * RDD java API http://blog.51cto.com/7639240/category1.html
 */
public class TopNJava {
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("topN-java");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        int topN = 10;
        int n = 1000000;
        String urlPath = "data/topn/" + n;
        String url1SavePath = "result/java/" + n;

        JavaRDD<String> rdd = sparkContext.textFile(urlPath);  // 读取文件，构建String类型RDD
        JavaPairRDD<String, Long> wordCountRDD = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s != null && !s.isEmpty();
            }
        }).flatMapToPair(new PairFlatMapFunction<String, String, Long>() {
            @Override
            public Iterator<Tuple2<String, Long>> call(String s) throws Exception {
                String[] temp = s.split(" ");
                List<Tuple2<String, Long>> wordCounts = new ArrayList<>();
                for (String word : temp) {
                    wordCounts.add(new Tuple2<>(word, 1L));
                }
                return wordCounts.iterator();
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaRDD<BoundedPriorityQueue<Tuple2<String, Long>>> mapRDDs =
                wordCountRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Long>>,
                        BoundedPriorityQueue<Tuple2<String, Long>>>() {
                    @Override
                    public Iterator<BoundedPriorityQueue<Tuple2<String, Long>>> call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                        WordCountComparator comparator = new WordCountComparator();
                        BoundedPriorityQueue<Tuple2<String, Long>> queue = new BoundedPriorityQueue<>(topN, comparator.reversed());
                        Ordering<Tuple2<String, Long>> ordering = new Ordering<Tuple2<String, Long>>() {

                            @Override
                            public int compare(@Nullable Tuple2<String, Long> stringLongTuple2, @Nullable Tuple2<String, Long> t1) {
                                return comparator.compare(stringLongTuple2, t1);
                            }
                        };
                        queue.addAll(ordering.leastOf(tuple2Iterator, topN));
                        return Collections.singletonList(queue).iterator();
                    }
                });

        List<Tuple2<String, Long>> result = new ArrayList<>();
        if (mapRDDs.partitions().size() > 0) {
            Iterator<Tuple2<String, Long>> resultIterator =
                    mapRDDs.reduce(new Function2<BoundedPriorityQueue<Tuple2<String, Long>>, BoundedPriorityQueue<Tuple2<String, Long>>, BoundedPriorityQueue<Tuple2<String, Long>>>() {
                        @Override
                        public BoundedPriorityQueue<Tuple2<String, Long>> call(BoundedPriorityQueue<Tuple2<String, Long>> v1, BoundedPriorityQueue<Tuple2<String, Long>> v2) throws Exception {
                            v1.addAll(v2);
                            return v1;
                        }
                    }).iterator();
            while (resultIterator.hasNext()) {
                result.add(resultIterator.next());
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2._2.compareTo(o1._2);
                }
            });
        }

        FileSystem.get(sparkContext.hadoopConfiguration()).delete(new Path(url1SavePath), true);
        sparkContext.parallelize(result, 1).saveAsTextFile(url1SavePath);

    }
}
