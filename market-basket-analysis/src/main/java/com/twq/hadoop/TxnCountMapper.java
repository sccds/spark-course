package com.twq.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TxnCountMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
    public long count = 0;
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        count += 1;
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(count), NullWritable.get());
    }
}

