package com.hadoop.learn.com.hadoop.learn.ch8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Ly on 2018/7/15.
 */
public class EmailPicker extends Configured implements Tool {

    public static class EmailMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, one);
        }
    }

    public static class EmailReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();
            int begin = keyString.indexOf('@');
            int end = keyString.indexOf('.');
            if (begin >= end) return;

            String name = keyString.substring(begin + 1, end);
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            multipleOutputs.write(key, new IntWritable(sum), name);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);
        if (job == null) return -1;

        job.setJarByClass(EmailPicker.class);
        job.setMapperClass(EmailMapper.class);
        job.setReducerClass(EmailReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new EmailPicker(), new String[]{"data/emails", "output"});
    }

}
