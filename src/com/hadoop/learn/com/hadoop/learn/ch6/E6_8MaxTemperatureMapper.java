package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @author Ly on 2018/7/6.
 */
public class E6_8MaxTemperatureMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

    private E6_7NcdcRecordParser parser = new E6_7NcdcRecordParser();

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        parser.parse(text);
        if (parser.isValidTemperature()) {
            outputCollector.collect(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
