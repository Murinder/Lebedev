package org.example.crossCorrelation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private Text keyOut = new Text();
    private MapWritable stripe = new MapWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] items = value.toString().trim().split("\\s+");
        if (items.length < 2) return;
        Arrays.sort(items);
        for (int i = 0; i < items.length; i++) {
            stripe.clear();
            keyOut.set(items[i]);
            for (int j = 0; j < items.length; j++) {
                if (i == j) continue;
                Text coItem = new Text(items[j]);
                LongWritable count = (LongWritable) stripe.get(coItem);
                if (count == null) {
                    stripe.put(coItem, new LongWritable(1));
                } else {
                    count.set(count.get() + 1);
                }
            }
            context.write(keyOut, stripe);
        }
    }
}