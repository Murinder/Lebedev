package org.example.crossCorrelation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.*;

public class HdfsRecommender {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HdfsRecommender <hdfs_output_dir> <item>");
            return;
        }

        String hdfsUri = "hdfs://localhost:9000"; // или из core-site.xml
        String outputDir = args[0];
        String target = args[1];

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.setInt("dfs.replication", 1);
        conf.setBoolean("dfs.permissions.enabled", false);
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

        Path dirPath = new Path(outputDir);
        Map<String, Integer> coOccur = new HashMap<>();

        for (FileStatus status : fs.listStatus(dirPath)) {
            Path path = status.getPath();
            if (path.getName().startsWith("part-")) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(path), "UTF-8"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // Формат Pairs: "A\tB\tcount"
                        String[] parts = line.split("\t");
                        if (parts.length == 3) {
                            String a = parts[0];
                            String b = parts[1];
                            int cnt = Integer.parseInt(parts[2]);
                            if (a.equals(target)) {
                                coOccur.put(b, coOccur.getOrDefault(b, 0) + cnt);
                            } else if (b.equals(target)) {
                                coOccur.put(a, coOccur.getOrDefault(a, 0) + cnt);
                            }
                        }
                    }
                }
            }
        }

        // Сортировка и вывод топ-10
        coOccur.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry -> System.out.println(entry.getKey() + " : " + entry.getValue()));
    }
}