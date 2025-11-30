package org.example.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AuthorityUpdateReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> inlinks = new ArrayList<>();
        double authSum = 0.0;
        double hub = 0.0;
        String outlinks = "";

        for (Text value : values) {
            String line = value.toString().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\t");
            if (parts.length == 0) continue;

            String recordType = parts[0];

            if (recordType.equals("structure")) {
                if (parts.length >= 4) {
                    outlinks = parts[1]; //Исходящие ссылки
                    hub = Double.parseDouble(parts[3]);
                }
            } else if (recordType.equals("auth_contrib")) {
                if (parts.length >= 2) {
                    try {
                        authSum += Double.parseDouble(parts[1]);
                    } catch (NumberFormatException e) {
                        System.err.println("Ошибка при разборе значения в auth_contrib: " + parts[1]);
                    }
                }
            } else if (recordType.equals("inlink")) {
                if (parts.length >= 2 && !parts[1].isEmpty()) {
                    inlinks.add(parts[1]);
                }
            }
        }

        //Записываем в формате: node<таб>outlinks<таб>auth_sum<таб>hub
        context.write(key, new Text(outlinks + "\t" + authSum + "\t" + hub));
    }
}