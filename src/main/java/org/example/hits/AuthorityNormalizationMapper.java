package org.example.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AuthorityNormalizationMapper extends Mapper<LongWritable, Text, Text, Text> {
    private double normFactor;
    
    @Override
    protected void setup(Context context) {
        normFactor = context.getConfiguration().getDouble("auth.norm.factor", 1.0);
        if (normFactor == 0) {
            normFactor = 1.0; //Избегаем деления на ноль
        }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        
        String[] parts = line.split("\t");
        //Формат: node<таб>outlinks<таб>auth<таб>hub
        if (parts.length < 4) return;
        
        String nodeId = parts[0];
        String outlinks = parts[1];
        double auth = parts.length > 2 ? Double.parseDouble(parts[2]) : 0.0;
        double hub = parts.length > 3 ? Double.parseDouble(parts[3]) : 0.0;
        
        //Нормализуем оценку авторитетности
        double normAuth = auth / normFactor;
        
        //Записываем в том же формате
        context.write(new Text(nodeId), new Text(outlinks + "\t" + normAuth + "\t" + hub));
    }
}