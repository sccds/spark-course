package com.twq.hadoop;

import com.twq.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 *  phase two：根据频繁项集生成关联规则的Reduce：
 *  map端的输出：
 (a,b,c -> 1)
 (a,b -> a,b,c;1)
 (a,c -> a,b,c;1)
 (b,c -> a,b,c;1)
 (b -> 4)
 (a,b,d -> 1)
 (a,b -> a,b,d;1)
 (a,d -> a,b,d;1)
 (b,d -> a,b,d;1)
 (b,d -> 1)
 (b -> b,d;1)
 (d -> b,d;1)
 (........)
 (d -> 1)
 reduce的输入是：
 (a,b,c -> 1)
 (b -> 4, b,d;1, a,b;2, b,c;3)
 (a,b -> a,b,c;1, a,b,d;1, 2)
 (b,d -> a,b,d;1, 1)
 (a,b,d -> 1)
 (a -> a,b;2, 2, a,d;1, a,c;1)
 (a,d -> a,b,d;1, 1)
 (b,c -> a,b,c;1, 3)
 (a,c -> a,b,c;1, 1)
 (c -> b,c;3, a,c;1), 3)
 (d -> b,d;1, a,d;1, 1)
 reduce的输出是：
 [a] => b,c	(0.250000, 0.500000)
 [a] => b,d	(0.250000, 0.500000)
 [a] => b	(0.500000, 1.000000)
 [a] => d	(0.250000, 0.500000)
 [a] => c	(0.250000, 0.500000)
 [a,b] => c	(0.250000, 0.500000)
 [a,b] => d	(0.250000, 0.500000)
 .............
 [d] => a	(0.250000, 1.000000)
 [d] => b	(0.250000, 1.000000)
 *
 */
public class AssociationRuleMiningReducer extends Reducer<Text, Text, Text, Text> {
    private double minConfidence;
    private HashSet<String> rules = new HashSet<String>();
    private Text ruleKey = new Text();
    private Text ruleValue = new Text();
    private long txnCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        minConfidence = conf.getDouble("MIN_CONFIDENCE", 0.1);
        txnCount = conf.getInt("TRANSACTION_COUNT", 1);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1、将前件项集的项解析出来放到一个列表中
        StringTokenizer antecedentST = new StringTokenizer(key.toString(), ",");
        ArrayList<String> antecedentItems = new ArrayList<String>(); // 前件项集
        while (antecedentST.hasMoreTokens())
            antecedentItems.add(antecedentST.nextToken());

        // 2、解析前件对应的value中的超集的信息
        // 定义一个Map，存放项集的超集以及超集出现的次数
        HashMap<String, Integer> itemSetMap = new HashMap<String, Integer>();
        int antecedentSupportCount = 0; // 定义前件的支持数
        // 循环values中的每一个值
        for (Text textItemSet : values) {
            // 使用分号对每一个value进行切割，并将切割后的元素放在一个数组中
            StringTokenizer itemStringTokenizer = new StringTokenizer(textItemSet.toString(), ";");
            String[] superItemSet = new String[itemStringTokenizer.countTokens()];
            int i = 0;
            while (itemStringTokenizer.hasMoreTokens())
                superItemSet[i++] = itemStringTokenizer.nextToken();
            // 如果数组的大小为1的话，则表示这个元素是前件的支持数
            if (superItemSet.length == 1) // Support of antecedent
                antecedentSupportCount = Integer.parseInt(superItemSet[0]);
            else {
                // 否则的话，表示前件所属的超集以及其出现的次数
                String itemSetKey = superItemSet[0]; // Super-set of antecedent
                int supportCount = Integer.parseInt(superItemSet[1]); // Support count of that super-set
                itemSetMap.put(itemSetKey, supportCount);
            }
        }

        // 3、挖掘关联规则以及规则验证
        // 循环前件的各种超集，然后根据前件生成各种后件
        for (Map.Entry<String, Integer> itemSet : itemSetMap.entrySet()) {
            // 解析出当前超集中各个项，并放到一个列表中
            StringTokenizer items = new StringTokenizer(itemSet.getKey(), ",");
                ArrayList<String> consequentItems = new ArrayList<String>(); //定义一个后件项集
            while (items.hasMoreTokens())
                consequentItems.add(items.nextToken());

            // 当前超集中的所有的项减去前件中所有的项就是后件
            consequentItems.removeAll(antecedentItems);
            // 规则的支持度等于当前超集出现的次数 / 总共的交易数
            double ruleSupport = 1.0 * itemSet.getValue() / txnCount;
            // 置信度等于规则当前超集出现的次数 / 前件的支持数
            double confidence = 1.0 * itemSet.getValue() / antecedentSupportCount;

            // 规则验证
            // 保留置信度大于最小的置信度的规则
            if (confidence >= minConfidence) {
                String antecedent = "[" + key.toString() + "]"; // 前件
                String consequent = Utils.list2string(consequentItems); // 后件
                String rule = antecedent + " => " + consequent; // 规则
                if (rules.contains(rule)) // 防止重复的规则保存
                    continue;
                rules.add(rule);
                ruleKey.set(rule);
                ruleValue.set("(" + String.format("%.6f", ruleSupport) + ", " + String.format("%.6f", confidence) + ")");
                context.write(ruleKey, ruleValue);
            }
        }
    }
}
