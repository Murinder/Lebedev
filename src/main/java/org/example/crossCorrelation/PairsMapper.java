package org.example.crossCorrelation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class PairsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text pair = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] items = value.toString().trim().split("\\s+");
        if (items.length < 2) return;
        Arrays.sort(items); // для упорядоченных пар (A,B) = (B,A)
        for (int i = 0; i < items.length; i++) {
            for (int j = i + 1; j < items.length; j++) {
                pair.set(items[i] + "\t" + items[j]);
                context.write(pair, one);
            }
        }
    }
}