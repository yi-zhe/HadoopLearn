package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Ly on 2018/7/6.
 */
public class E6_2 {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        System.out.println(conf.get("fs.defaultFS"));
    }

}
