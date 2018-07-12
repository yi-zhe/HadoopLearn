package com.hadoop.learn.com.hadoop.learn.ch8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Ly on 2018/7/13.
 */
public class MinimalMapReduce extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        JobConf jobConf = new JobConf(getConf());
        jobConf.setJarByClass(getClass());
        FileInputFormat.addInputPath(jobConf, new Path("data/weather"));
        FileOutputFormat.setOutputPath(jobConf, new Path("output"));

        Job job = new Job(jobConf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MinimalMapReduce(), args);
    }
}
