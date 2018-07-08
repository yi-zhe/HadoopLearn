package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author Ly on 2018/7/8.
 */
public class E6_11 {

    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("./data/weather");
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        E6_10MaxTemperatureDriver driver = new E6_10MaxTemperatureDriver();
        driver.setConf(conf);

        int code = driver.run(new String[]{input.toString(), output.toString()});
    }

}
