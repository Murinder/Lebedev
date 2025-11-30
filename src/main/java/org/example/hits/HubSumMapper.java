package org.example.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HubSumMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        
        //Формат: node<таб>outlinks<таб>auth<таб>hub
        String[] parts = line.split("\t");
        if (parts.length < 4) return;
        
        //parts[3] - посредническая оценка
        try {
            double hub = Double.parseDouble(parts[3]);
            double hubSquared = hub * hub;
            context.write(new Text("hub_sum"), new Text(String.valueOf(hubSquared)));
        } catch (NumberFormatException e) {
            System.err.println("Ошибка при разборе значения посреднической оценки: " + parts[3]);
        }
    }
}