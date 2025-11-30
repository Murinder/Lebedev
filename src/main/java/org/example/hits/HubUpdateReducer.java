package org.example.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class HubUpdateReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String outlinks = "";
        double auth = 0.0;
        double hubSum = 0.0;
        
        for (Text value : values) {
            String line = value.toString().trim();
            if (line.isEmpty()) continue;
            
            String[] parts = line.split("\t");
            if (parts.length == 0) continue;
            
            String recordType = parts[0];
            
            if (recordType.equals("structure")) {
                if (parts.length >= 4) {
                    outlinks = parts[1];
                    auth = Double.parseDouble(parts[2]);
                }
            } else if (recordType.equals("hub_contrib")) {
                if (parts.length >= 2) {
                    try {
                        hubSum += Double.parseDouble(parts[1]);
                    } catch (NumberFormatException e) {
                        System.err.println("Ошибка при разборе значения в hub_contrib: " + parts[1]);
                    }
                }
            }
        }
        
        //Записываем в формате: node<таб>outlinks<таб>auth<таб>hub_sum
        context.write(key, new Text(outlinks + "\t" + auth + "\t" + hubSum));
    }
}