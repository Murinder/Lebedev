package org.example.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AuthoritySumMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        //Формат: node<таб>outlinks<таб>auth<таб>hub
        String[] parts = line.split("\t");
        if (parts.length < 3) return;

        //parts[2] - оценка авторитетности
        try {
            if (parts.length >= 3) {
                double auth = Double.parseDouble(parts[2]);
                double authSquared = auth * auth;
                context.write(new Text("auth_sum"), new Text(String.valueOf(authSquared)));
            }
        } catch (NumberFormatException e) {
            System.err.println("Ошибка при разборе значения авторитетности: " + (parts.length >= 3 ? parts[2] : ""));
        }
    }
}