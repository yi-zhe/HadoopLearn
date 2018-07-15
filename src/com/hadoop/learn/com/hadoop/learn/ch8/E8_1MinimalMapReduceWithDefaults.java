package com.hadoop.learn.com.hadoop.learn.ch8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Ly on 2018/7/13.
 */
public class E8_1MinimalMapReduceWithDefaults extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);
        if (job == null) return -1;

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Mapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(HashPartitioner.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MinimalMapReduce(), args);
    }
}
