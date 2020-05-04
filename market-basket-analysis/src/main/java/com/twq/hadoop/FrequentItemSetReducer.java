package com.twq.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *  phase one：生成频繁项集的Reduce：
 *  输入为Map阶段的输出:
 (a -> 1)
 (b -> 1)
 (c -> 1)
 (a,b -> 1)
 (a,c -> 1)
 (b,c -> 1)
 (a,b,c -> 1)
 (a -> 1)
 (b -> 1)
 (d -> 1)
 (a,b -> 1)
 (a,d -> 1)
 (b,d -> 1)
 (a,b,d -> 1)
 (b -> 1)
 (c -> 1)
 (b,c -> 1)
 (b -> 1)
 (c -> 1)
 (b,c -> 1)
 *  输出为(假设minSupportCount=0)：
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
 */
public class FrequentItemSetReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private int minSupportCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        minSupportCount = context.getConfiguration().getInt("MIN_SUPPORT_COUNT", 2);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        // 处理map输出的每一个项集以及这个项集出现的次数(1)
        Long supportCount = 0L;
        // 对相同相同出现的次数进行累加，得到这个项集出现的总次数
        for (LongWritable v : values) {
            supportCount += v.get();
        }
        // 如果当前这个项集累计出现的次数比最小的支持数还要小，则过滤掉，不进行保存
        if (supportCount >= minSupportCount) {
            context.write(key, new LongWritable(supportCount));
        }
    }

}
