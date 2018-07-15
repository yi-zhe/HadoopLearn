package com.hadoop.learn.com.hadoop.learn.ch8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * @author Ly on 2018/7/13.
 */
public class JobBuilder {

    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws Exception {
        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }

        JobConf jobConf = new JobConf(conf);
        jobConf.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        return new Job(jobConf);
    }

    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

}
