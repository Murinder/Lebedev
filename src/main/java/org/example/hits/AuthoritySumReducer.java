package org.example.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AuthoritySumReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;

        for (Text value : values) {
            try {
                sum += Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                System.err.println("Ошибка при разборе значения: " + value);
            }
        }

        double normFactor = Math.sqrt(sum > 0 ? sum : 1.0); //Защита от деления на ноль
        context.write(key, new Text(String.valueOf(normFactor)));
    }
}