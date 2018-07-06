package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

/**
 * @author Ly on 2018/7/6.
 */
public class E6_5 {

    @Test
    public void processValidRecord() throws Exception {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + "99999V0203201N00261220001CN9999999N9-00111+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new E6_6MaxTemperature())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void ignoresMissingRecord() throws Exception {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + "99999V0203201N00261220001CN9999999N9+99991+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new E6_6MaxTemperature())
                .withInput(new LongWritable(0), value)
                .runTest();
    }

    @Test
    public void ignoresMissingRecord2() throws Exception {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + "99999V0203201N00261220001CN9999999N9+99991+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new E6_8MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }
}
