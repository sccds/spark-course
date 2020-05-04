package com.twq.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TxnCountReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
    public long count = 0;
    public void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        count += key.get();
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(count), NullWritable.get());
    }
}
