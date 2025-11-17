package org.example.crossCorrelation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Long> total = new HashMap<>();
        for (MapWritable stripe : values) {
            for (Writable k : stripe.keySet()) {
                String coItem =  k.toString();
                LongWritable cnt = (LongWritable) stripe.get(k);
                total.merge(coItem, cnt.get(), Long::sum);
            }
        }

        // Сортируем
        List<Map.Entry<String, Long>> list = new ArrayList<>(total.entrySet());
        list.sort(Map.Entry.<String, Long>comparingByValue().reversed());

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> e : list) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(e.getKey()).append(":").append(e.getValue());
        }

        context.write(key, new Text(sb.toString()));
    }
}