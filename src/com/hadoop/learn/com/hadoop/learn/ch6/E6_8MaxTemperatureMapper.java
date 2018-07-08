package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Ly on 2018/7/6.
 */
public class E6_8MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private E6_7NcdcRecordParser parser = new E6_7NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
    }
}
