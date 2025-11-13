package org.example.crossCorrelation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsRecommender {
    // Регулярное выражение для пары "ключ:значение", например: product_042:12
    private static final Pattern PAIR_PATTERN = Pattern.compile("([^,:\\s]+):(\\d+)");

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HdfsRecommender <hdfs_stripes_output_dir> <item>");
            return;
        }

        String hdfsUri = "hdfs://localhost:9000";
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
                        // Формат Stripes: "A\tB:cnt1, C:cnt2, D:cnt3"
                        String[] parts = line.split("\t", 2); // разделяем только на 2 части: ключ и всё остальное
                        if (parts.length != 2) continue;

                        String key = parts[0];
                        String stripeStr = parts[1].trim();

                        if (key.equals(target)) {
                            // Парсим "B:12, C:5, D:3" → обновляем coOccur
                            Matcher m = PAIR_PATTERN.matcher(stripeStr);
                            while (m.find()) {
                                String neighbor = m.group(1);
                                int count = Integer.parseInt(m.group(2));
                                if (!neighbor.equals(target)) {
                                    coOccur.merge(neighbor, count, Integer::sum);
                                }
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