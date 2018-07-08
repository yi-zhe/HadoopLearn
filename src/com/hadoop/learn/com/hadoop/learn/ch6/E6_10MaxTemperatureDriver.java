package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Ly on 2018/7/7.
 */
public class E6_10MaxTemperatureDriver extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        Job job = new Job(getConf(), "Max Temperature");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapperClass(E6_8MaxTemperatureMapper.class);
        job.setCombinerClass(E6_9MaxTemperatureReducer.class);
        job.setReducerClass(E6_9MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int code = ToolRunner.run(new E6_10MaxTemperatureDriver(), new String[]{"./data/weather", "output"});

        System.exit(code);
    }
}
