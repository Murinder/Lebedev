package org.example.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AuthorityUpdateMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split("\t");
        if (parts.length < 4) return;

        String nodeId = parts[0];
        String neighborsStr = parts[1];
        String[] neighbors = neighborsStr.isEmpty() ? new String[0] : neighborsStr.split(",");
        double auth = Double.parseDouble(parts[2]);
        double hub = Double.parseDouble(parts[3]);

        //Отправляем информацию о структуре
        context.write(new Text(nodeId), new Text("structure\t" + neighborsStr + "\t" + auth + "\t" + hub));

        //Для каждой смежной вершины отправляем вклад в авторитетность
        for (String neighbor : neighbors) {
            if (!neighbor.isEmpty()) {
                context.write(new Text(neighbor), new Text("auth_contrib\t" + hub));
                context.write(new Text(neighbor), new Text("inlink\t" + nodeId));
            }
        }
    }
}