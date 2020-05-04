package com.twq.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *  MapReduce实现的Market Basket Analysis的Driver程序
 *  主要是包含三个MapReduce jobs：
 *  1、计算原始交易数据的总大小
 *  2、生成频繁项集
 *  3、根据频繁项集生成关联规则
 */
public class MBADriver extends Configured implements Tool {
    private final static String USAGE = "USAGE %s: <input dir path> <output dir path> <min. support> <min. confidence> <transaction delimiter>\n";
    private static String defFS; // HDFS的基本路径
    private static String inputDir; // 原始交易数据集在HDFS上的路径目录
    private static String outputDir; // 中间数据以及最终结果数据的输出的HDFS目录
    private static int txnCount; // 原始交易数据的总大小
    private static double minSupport;   // 最小支持度
    private static double minConfidence;    // 最小置信度
    private static String delimiter; // 原始交易数据的每一个item之间的分隔符

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MBADriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length < 5) {
            System.err.printf("Invalid arguments!\n"+USAGE, getClass().getName());
            return 1;
        }
        inputDir = args[0];
        outputDir = args[1];
        minSupport = Double.parseDouble(args[2]);
        minConfidence = Double.parseDouble(args[3]);
        delimiter = args[4];
        Configuration conf = new Configuration();
        defFS = conf.get("fs.defaultFS");
        //计算总共的交易的数量
        txnCount = countTransaction();
        // 根据最小支持度和交易数据总大小计算最小支持数
        int minSupportCount = (int)Math.ceil(minSupport * txnCount);
        // 生成频繁项集
        jobFrequentItemsetMining(minSupportCount);
        // 根据频繁项集生成关联规则
        jobAssociationRuleMining();

        return 0;
    }

    private void jobFrequentItemsetMining(int minSupportCount) throws IOException, ClassNotFoundException, InterruptedException {
        String hdfsInputPath = defFS + inputDir;
        String hdfsOutputPath = defFS + outputDir + "/all-frequent-itemsets/freq-list";
        Configuration config = new Configuration();
        // config的设置一定要是在Job的初始化之前执行
        config.setInt("MIN_SUPPORT_COUNT", minSupportCount);
        config.set("DELIMITER", delimiter);

        Job job = Job.getInstance(config, "jobFrequentItemsetMining");
        job.setJarByClass(MBADriver.class);
        job.setMapperClass(FrequentItemSetMapper.class);
        job.setReducerClass(FrequentItemSetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileSystem.get(config).delete(new Path(hdfsOutputPath), true);
        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        boolean success = job.waitForCompletion(true);
        if(!success)
            throw new IllegalStateException("jobFrequentItemsetMining failed!");
    }

    private void jobAssociationRuleMining() throws IOException, ClassNotFoundException, InterruptedException {
        String hdfsInputPath = defFS + outputDir + "/all-frequent-itemsets/freq-list";
        String hdfsOutputPath = defFS + outputDir + "/rule-mining-output";
        Configuration config = new Configuration();
        config.setDouble("MIN_CONFIDENCE", minConfidence);
        config.setInt("TRANSACTION_COUNT", txnCount);

        Job job = Job.getInstance(config, "jobAssociationRuleMining");
        job.setJarByClass(MBADriver.class);
        job.setMapperClass(AssociationRuleMiningMapper.class);
        job.setReducerClass(AssociationRuleMiningReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem.get(config).delete(new Path(hdfsOutputPath), true);
        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        boolean success = job.waitForCompletion(true);
        if(!success)
            throw new IllegalStateException("jobAssociationRuleMining failed!");
    }

    private int countTransaction() throws IOException, ClassNotFoundException, InterruptedException {
        String hdfsInputPath = defFS + inputDir;
        String hdfsOutputPath = defFS + outputDir + "/transaction-count";
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Transaction Counter");

        job.setJarByClass(MBADriver.class);
        job.setMapperClass(TxnCountMapper.class);
        job.setReducerClass(TxnCountReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        FileSystem.get(config).delete(new Path(hdfsOutputPath), true);
        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));
        boolean success = job.waitForCompletion(true);
        if(!success)
            throw new IllegalStateException("Transaction Counter failed!");

        FileSystem fileSystem = FileSystem.get(config);
        FileStatus fileStatus = fileSystem.listStatus(new Path(hdfsOutputPath))[0];
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));
        String countStr = bufferedReader.readLine().trim();
        return Integer.parseInt(countStr);
    }
}
