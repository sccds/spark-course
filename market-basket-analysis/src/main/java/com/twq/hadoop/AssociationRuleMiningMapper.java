package com.twq.hadoop;

import com.twq.util.Combination;
import com.twq.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 *  phase two：根据频繁项集生成关联规则的Map：
 *  其输入为 phase one的输出，所以这个map需要从hdfs文件defFS + outputDir + "/all-frequent-itemsets/freq-list"中读取数据
 *  其逻辑为：
 给定一个频繁模式：(K=List<A1,A2,...,An>,V=Frequency)
 创建如下的子模式(K2,V2)
 (K2=K=List<A1,A2,...,An>,V2=Tuple(null,V))
 即把K作为K2，Tuple(null,V))作为V2
 (K2=List<A1,A2,...,An-1>),V2=Tuple(K,V))
 (K2=List<A1,A2,...,An-2,An>),V2=Tuple(K,V))
 ...
 (K2=List<A2,...,An-1,An>),V2=Tuple(K,V))
 即把K的每一个元素拿掉一次作为K2,Tuple(K,V))作为V2

 输入为：
 (a,b,c -> 1)
 (b -> 4)
 (a,b,d -> 1)
 (b,d -> 1)
 (a,b -> 2)
 (a -> 2)
 (a,d -> 1)
 (b,c -> 3)
 (a,c -> 1)
 (c -> 3)
 (d -> 1)
 输出为：
 (a,b,c -> 1)
 (a,b -> a,b,c;1)
 (a,c -> a,b,c;1)
 (b,c -> a,b,c;1)
 (b -> 4)
 (a,b,d -> 1)
 (a,b -> a,b,d;1)
 (a,d -> a,b,d;1)
 (b,c -> a,b,d;1)
 (b,d -> 1)
 (b -> b,d;1)
 (d -> b,d;1)
 (........)
 (d -> 1)
 */
public class AssociationRuleMiningMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 比如value的数据为：a,b,c\t1
        String[] pair = value.toString().split("\t"); // 从phase one输出的文件中都出来的数据按照制表符进行切割
        String itemSet = pair[0].trim();    // 第一个元素是项集 (a,b,c)
        String supportCount = pair[1].trim(); // 第二个元素是这个项集出现的次数 (1)
        // 对项集按照逗号切割，得到项集中所有的项
        StringTokenizer item = new StringTokenizer(itemSet, ",");
        List<String> items = new ArrayList<>(); // List(a,b,c)
        while(item.hasMoreTokens())
            items.add(item.nextToken());
        // 将当前的项集以及对应的出现的次数，输出到reduce端
        keyOut.set(itemSet);
        valueOut.set(supportCount);
        // a,b,c -> 1
        context.write(keyOut, valueOut);
        // 如果项集中不止一个项的话，则需要创建子项集
        if(items.size() > 1) {
            // 循环当前的项集，然后计算所有的子项集
            List<List<String>> allSubItemSets = Combination.findSortedCombinations(items);
            allSubItemSets.remove(0);
            allSubItemSets.remove(allSubItemSets.size() - 1);
            for(List<String> subItemSet : allSubItemSets) {
                // 将子项集最后的,号用空格替代掉，然后输出到Reduce端
                // 输出的格式是：
                //      a,b -> a,b,c;1
                //      a,c -> a,b,c;1
                //      b,c -> a,b,c;1
                keyOut.set(Utils.list2string(subItemSet));
                valueOut.set(itemSet+";"+supportCount);
                context.write(keyOut, valueOut);
            }
        }
    }
}
