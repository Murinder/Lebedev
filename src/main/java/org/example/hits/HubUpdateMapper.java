package org.example.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HubUpdateMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> nodeOutlinks = new HashMap<>();
    private Map<String, Double> nodeAuth = new HashMap<>();
    private Map<String, Double> nodeHub = new HashMap<>();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        
        String[] parts = line.split("\t");
        if (parts.length < 4) return;
        
        String nodeId = parts[0];
        String outlinksStr = parts[1];
        double auth = Double.parseDouble(parts[2]);
        double hub = Double.parseDouble(parts[3]);
        
        //Сохраняем информацию об узле
        nodeOutlinks.put(nodeId, outlinksStr);
        nodeAuth.put(nodeId, auth);
        nodeHub.put(nodeId, hub);
        
        //Отправляем информацию о структуре
        context.write(new Text(nodeId), new Text("structure\t" + outlinksStr + "\t" + auth + "\t" + hub));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //Для каждого узла отправляем информацию о хаб-вкладах
        for (String nodeId : nodeOutlinks.keySet()) {
            String outlinksStr = nodeOutlinks.get(nodeId);
            String[] outlinks = outlinksStr.isEmpty() ? new String[0] : outlinksStr.split(",");
            
            for (String outlink : outlinks) {
                if (!outlink.isEmpty()) {
                    //Проверяем, существует ли такой узел
                    if (nodeAuth.containsKey(outlink)) {
                        double auth = nodeAuth.get(outlink);
                        context.write(new Text(nodeId), new Text("hub_contrib\t" + auth));
                    }
                }
            }
        }
    }
}