package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * @author Ly on 2018/7/6.
 * Tool ToolRunner
 */
public class E6_4 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new E6_4(), args);
        System.exit(code);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }
}
