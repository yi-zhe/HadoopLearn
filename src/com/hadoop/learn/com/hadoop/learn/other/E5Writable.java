package com.hadoop.learn.com.hadoop.learn.other;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * @author Ly on 2018/6/27.
 */
public class E5Writable {

    private static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    private static void deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
    }

    public static void main(String[] args) throws Exception {
        IntWritable writable = new IntWritable(163);
        byte[] bytes = serialize(writable);

        System.out.println(bytes.length);

        IntWritable newWritable = new IntWritable();
        deserialize(newWritable, bytes);
        System.out.println(newWritable.get());
    }
}
