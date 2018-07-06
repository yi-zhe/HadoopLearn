package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Ly on 2018/7/6.
 */
public class E6_9MaxTemperatureReducer implements Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text text, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int maxValue = Integer.MIN_VALUE;
        while (values.hasNext()) {
            maxValue = Math.max(maxValue, values.next().get());
        }
        outputCollector.collect(text, new IntWritable(maxValue));
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
