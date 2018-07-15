package com.hadoop.learn.com.hadoop.learn.ch9;

import com.hadoop.learn.com.hadoop.learn.ch6.E6_7NcdcRecordParser;
import com.hadoop.learn.com.hadoop.learn.ch6.E6_9MaxTemperatureReducer;
import com.hadoop.learn.com.hadoop.learn.ch8.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Ly on 2018/7/16.
 */
public class E9_1MaxTemperatureMapperWithCounters extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new E9_1MaxTemperatureMapperWithCounters(), new String[]{"data/weather", "output"});
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);
        if (job == null) return -1;

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(E6_9MaxTemperatureReducer.class);
        job.setReducerClass(E6_9MaxTemperatureReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    enum Temperature {
        MISSING,
        MALFORMED
    }

    static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {
        private E6_7NcdcRecordParser parser = new E6_7NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);

            if (parser.isValidTemperature()) {
                int airTemperature = parser.getAirTemperature();
                context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
            } else if (parser.isMalformed()) {
                System.err.println("Ignoring possibly corrupt input:" + value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            } else if (parser.isMissingTemperature()) {
                context.getCounter(Temperature.MISSING).increment(1);
            }

            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }

}
