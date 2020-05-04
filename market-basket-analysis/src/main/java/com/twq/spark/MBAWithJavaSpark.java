package com.twq.spark;

import com.twq.util.Combination;
import com.twq.util.Utils;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 使用Spark core 实现Market Basket Analysis
 */
public class MBAWithJavaSpark {
    public static void main(String[] args) throws Exception {
        final double minSupport = 0.40;
        final double minConfidence = 0.60;
        String delimiter = ",";
        String transactionsFileName = "market-basket-analysis/dataset/test/test.csv";

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Market-Basket-Analysis");
        sparkConf.setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> transactions = sparkContext.textFile(transactionsFileName, 1);

        long txnCount = transactions.count();
        double minSupportCount = txnCount * minSupport;

        JavaPairRDD<List<String>, Integer> itemSetCountRDD =
                transactions.flatMapToPair(new PairFlatMapFunction<String, List<String>, Integer>() {
                    @Override
                    public Iterator<Tuple2<List<String>, Integer>> call(String transaction) throws Exception {
                        List<String> list = Utils.toList(transaction, delimiter);
                        List<List<String>> subItemSets = Combination.findSortedCombinations(list);
                        List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
                        for (List<String> subItemSet : subItemSets) {
                            if (subItemSet.size() > 0) {
                                result.add(new Tuple2<>(subItemSet, 1));
                            }
                        }
                        return result.iterator();
                    }
                });

        // 生成了频繁项集
        JavaPairRDD<List<String>, Integer> frequentItemSetRDD = itemSetCountRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        }).filter(new Function<Tuple2<List<String>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<List<String>, Integer> v1) throws Exception {
                return v1._2() >= minSupportCount;
            }
        });

        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subItemSetRDD = frequentItemSetRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<List<String>, Integer>, List<String>, Tuple2<List<String>, Integer>>() {
                    @Override
                    public Iterator<Tuple2<List<String>, Tuple2<List<String>, Integer>>> call(Tuple2<List<String>, Integer> frequentItemSet) throws Exception {
                        List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<>();
                        List<String> itemSet = frequentItemSet._1;
                        Integer itemSetSupportCount = frequentItemSet._2;
                        result.add(new Tuple2<>(itemSet, new Tuple2<>(null, itemSetSupportCount)));  // 主要为了后面区分开，号拿到父集出现的次数
                        if (itemSet.size() == 1) {
                            return result.iterator();
                        }
                        List<List<String>> allSubItemSets = Combination.findSortedCombinations(itemSet);
                        allSubItemSets.remove(0);
                        allSubItemSets.remove(allSubItemSets.size() - 1);  // 删掉空的，和全部的
                        for (List<String> subItemSet : allSubItemSets) {
                            result.add(new Tuple2<List<String>, Tuple2<List<String>, Integer>>(subItemSet,
                                    new Tuple2<>(itemSet, itemSetSupportCount)));
                        }
                        return result.iterator();
                    }
                }
        );

        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subItemSetRDD.groupByKey();

        JavaRDD<String> associationRulesRDD = rules.flatMap(new FlatMapFunction<Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>>, String>() {
            @Override
            public Iterator<String> call(Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>> rule) throws Exception {
                List<String> result = new ArrayList<>();
                List<String> antecedent = rule._1;
                Iterable<Tuple2<List<String>, Integer>> inSuperSets = rule._2;
                List<Tuple2<List<String>, Integer>> superItemSets = new ArrayList<>();
                Integer antecedentSupportCount = 0;
                for (Tuple2<List<String>, Integer> t2 : inSuperSets) {
                    // find the "count" object
                    if (t2._1 == null) {
                        antecedentSupportCount = t2._2;
                    } else {
                        superItemSets.add(t2);
                    }
                }
                if (superItemSets.isEmpty()) {
                    return result.iterator();
                }
                for (Tuple2<List<String>, Integer> t2 : superItemSets) {
                    // 计算后件 = superItemSet - 前件
                    List<String> consequent = new ArrayList<>(t2._1);
                    consequent.removeAll(antecedent);

                    double ruleSupport = (double) t2._2 / (double) txnCount;
                    double confidence = (double) t2._2 / (double) antecedentSupportCount;

                    if (confidence >= minConfidence && antecedent != null && consequent != null) {
                        result.add("(" + antecedent + "=>" + consequent + "," + ruleSupport + "," + confidence + ")");
                    }
                }
                return result.iterator();
            }
        });

        FileSystem.get(sparkContext.hadoopConfiguration()).delete(new Path("market-basket-analysis/output-java/association_rules_with_conf"), true);
        associationRulesRDD.saveAsTextFile("market-basket-analysis/output-java/association_rules_with_conf");
        sparkContext.stop();
    }
}
