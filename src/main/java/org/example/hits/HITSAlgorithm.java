package org.example.hits;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;

public class HITSAlgorithm {

    // Шаг 1: Инвертирование графа для эффективной обработки входящих и исходящих ссылок
    public static class GraphInversionMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split("\\s+");
            if (parts.length < 4) return;
            
            String nodeId = parts[0];
            String neighborsStr = parts[1];
            double hubScore = Double.parseDouble(parts[2]);
            double authScore = Double.parseDouble(parts[3]);
            
            // Отправляем информацию о самой вершине
            context.write(new Text(nodeId), new Text("NODE:" + line));
            
            // Для каждой соседней вершины отправляем информацию о ссылке
            if (!neighborsStr.equals("-")) { // "-" означает отсутствие соседей
                String[] neighbors = neighborsStr.split(",");
                for (String neighbor : neighbors) {
                    String trimmedNeighbor = neighbor.trim();
                    if (!trimmedNeighbor.isEmpty()) {
                        context.write(new Text(trimmedNeighbor), new Text("LINK:" + nodeId));
                    }
                }
            }
        }
    }

    public static class GraphInversionReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> inLinks = new ArrayList<>();
            String nodeInfo = null;
            
            for (Text value : values) {
                String strValue = value.toString();
                if (strValue.startsWith("LINK:")) {
                    inLinks.add(strValue.substring(5));
                } else if (strValue.startsWith("NODE:")) {
                    nodeInfo = strValue.substring(5);
                }
            }
            
            if (nodeInfo != null) {
                String[] parts = nodeInfo.split("\\s+");
                // parts[0] - id вершины
                // parts[1] - исходящие ссылки (соседи)
                // parts[2] - hub score
                // parts[3] - auth score
                
                // Формат вывода: id вершины, исходящие ссылки, входящие ссылки, hub score, auth score
                String outLinks = parts[1];
                String inLinksStr = String.join(",", inLinks);
                if (inLinksStr.isEmpty()) {
                    inLinksStr = "-";
                }
                
                String invertedNode = parts[0] + " " + outLinks + " " + inLinksStr + " " + parts[2] + " " + parts[3];
                context.write(key, new Text(invertedNode));
            }
        }
    }

    // Шаг 2: Обновление оценок авторитетности
    public static class AuthorityUpdateMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split("\\s+");
            if (parts.length < 5) return;
            
            String nodeId = parts[0];
            String outLinks = parts[1];
            String inLinks = parts[2];
            double hubScore = Double.parseDouble(parts[3]);
            
            // Отправляем текущую вершину для сохранения информации
            context.write(new Text(nodeId), new Text("NODE:" + line));
            
            // Для каждой вершины, на которую ссылается данная вершина, отправляем hub score
            if (!outLinks.equals("-")) {
                String[] neighbors = outLinks.split(",");
                for (String neighbor : neighbors) {
                    String trimmedNeighbor = neighbor.trim();
                    if (!trimmedNeighbor.isEmpty()) {
                        context.write(new Text(trimmedNeighbor), new Text("HUB:" + hubScore));
                    }
                }
            }
        }
    }

    public static class AuthorityUpdateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double newAuthScore = 0.0;
            String nodeInfo = null;
            
            for (Text value : values) {
                String strValue = value.toString();
                if (strValue.startsWith("HUB:")) {
                    double hubScore = Double.parseDouble(strValue.substring(4));
                    newAuthScore += hubScore;
                } else if (strValue.startsWith("NODE:")) {
                    nodeInfo = strValue.substring(5);
                }
            }
            
            if (nodeInfo != null) {
                String[] parts = nodeInfo.split("\\s+");
                // parts[0] - id вершины
                // parts[1] - исходящие ссылки
                // parts[2] - входящие ссылки
                // parts[3] - hub score (не меняется на этом шаге)
                // parts[4] - старый auth score (заменяем на новый)
                
                String updatedNode = parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3] + " " + newAuthScore;
                context.write(key, new Text(updatedNode));
            }
        }
    }

    // Шаг 3: Обновление посреднических оценок
    public static class HubUpdateMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split("\\s+");
            if (parts.length < 5) return;
            
            String nodeId = parts[0];
            String outLinks = parts[1];
            String inLinks = parts[2];
            double authScore = Double.parseDouble(parts[4]);
            
            // Отправляем текущую вершину для сохранения информации
            context.write(new Text(nodeId), new Text("NODE:" + line));
            
            // Для каждой вершины, которая ссылается на данную вершину, отправляем auth score
            if (!inLinks.equals("-")) {
                String[] neighbors = inLinks.split(",");
                for (String neighbor : neighbors) {
                    String trimmedNeighbor = neighbor.trim();
                    if (!trimmedNeighbor.isEmpty()) {
                        context.write(new Text(trimmedNeighbor), new Text("AUTH:" + authScore));
                    }
                }
            }
        }
    }

    public static class HubUpdateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double newHubScore = 0.0;
            String nodeInfo = null;
            
            for (Text value : values) {
                String strValue = value.toString();
                if (strValue.startsWith("AUTH:")) {
                    double authScore = Double.parseDouble(strValue.substring(5));
                    newHubScore += authScore;
                } else if (strValue.startsWith("NODE:")) {
                    nodeInfo = strValue.substring(5);
                }
            }
            
            if (nodeInfo != null) {
                String[] parts = nodeInfo.split("\\s+");
                // parts[0] - id вершины
                // parts[1] - исходящие ссылки
                // parts[2] - входящие ссылки
                // parts[3] - старый hub score (заменяем на новый)
                // parts[4] - auth score (не меняется на этом шаге)
                
                String updatedNode = parts[0] + " " + parts[1] + " " + parts[2] + " " + newHubScore + " " + parts[4];
                context.write(key, new Text(updatedNode));
            }
        }
    }

    // Шаг 4: Нормализация значений
    public static class NormalizationSumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split("\\s+");
            if (parts.length < 5) return;
            
            double hubScore = Double.parseDouble(parts[3]);
            double authScore = Double.parseDouble(parts[4]);
            
            // Отправляем квадраты значений для вычисления нормализующих коэффициентов
            context.write(new Text("HUB_SQ_SUM"), new DoubleWritable(hubScore * hubScore));
            context.write(new Text("AUTH_SQ_SUM"), new DoubleWritable(authScore * authScore));
            
            // Отправляем саму вершину для сохранения
            context.write(new Text(parts[0]), new DoubleWritable(Double.parseDouble(String.valueOf(value))));
        }
    }

    public static class NormalizationSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            
            // Вычисляем корень квадратный из суммы квадратов
            double normFactor = Math.sqrt(sum);
            context.write(key, new DoubleWritable(normFactor));
        }
    }

    public static class NormalizationApplyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private double hubNormFactor = 1.0;
        private double authNormFactor = 1.0;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Получаем нормализующие коэффициенты из конфигурации
            Configuration conf = context.getConfiguration();
            hubNormFactor = conf.getDouble("hub.norm.factor", 1.0);
            authNormFactor = conf.getDouble("auth.norm.factor", 1.0);
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split("\\s+");
            if (parts.length < 5) return;
            
            String nodeId = parts[0];
            String outLinks = parts[1];
            String inLinks = parts[2];
            double hubScore = Double.parseDouble(parts[3]);
            double authScore = Double.parseDouble(parts[4]);
            
            // Применяем нормализацию
            double normalizedHub = hubScore / hubNormFactor;
            double normalizedAuth = authScore / authNormFactor;
            
            String normalizedNode = nodeId + " " + outLinks + " " + inLinks + " " + normalizedHub + " " + normalizedAuth;
            context.write(new Text(nodeId), new Text(normalizedNode));
        }
    }

    // Основной метод для запуска алгоритма
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: HITSAlgorithm <input path> <output path> <max iterations> <use normalization>");
            System.exit(1);
        }
        
        String inputPath = args[0];
        String outputPath = args[1];
        int maxIterations = Integer.parseInt(args[2]);
        boolean useNormalization = Boolean.parseBoolean(args[3]);
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        // Шаг 1: Инвертирование графа
        Path invertedGraphPath = new Path(outputPath + "/inverted-graph");
        
        Job inversionJob = Job.getInstance(conf, "Graph Inversion");
        inversionJob.setJarByClass(HITSAlgorithm.class);
        inversionJob.setMapperClass(GraphInversionMapper.class);
        inversionJob.setReducerClass(GraphInversionReducer.class);
        inversionJob.setOutputKeyClass(Text.class);
        inversionJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(inversionJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(inversionJob, invertedGraphPath);
        
        if (!inversionJob.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // Итерации алгоритма HITS
        Path currentInput = invertedGraphPath;
        
        for (int i = 0; i < maxIterations; i++) {
            System.out.println("Starting iteration " + i);
                        // Этап 1: Обновление оценок авторитетности
            Path authorityOutput = new Path(outputPath + "/iteration-" + i + "-authority");
            
            Job authorityJob = Job.getInstance(conf, "HITS Authority Update Iteration " + i);
            authorityJob.setJarByClass(HITSAlgorithm.class);
            authorityJob.setMapperClass(AuthorityUpdateMapper.class);
            authorityJob.setReducerClass(AuthorityUpdateReducer.class);
            authorityJob.setOutputKeyClass(Text.class);
            authorityJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(authorityJob, currentInput);
            FileOutputFormat.setOutputPath(authorityJob, authorityOutput);
            
            if (!authorityJob.waitForCompletion(true)) {
                System.exit(1);
            }
            
            // Этап 2: Обновление посреднических оценок
            Path hubOutput = new Path(outputPath + "/iteration-" + i + "-hub");
            
            Job hubJob = Job.getInstance(conf, "HITS Hub Update Iteration " + i);
            hubJob.setJarByClass(HITSAlgorithm.class);
            hubJob.setMapperClass(HubUpdateMapper.class);
            hubJob.setReducerClass(HubUpdateReducer.class);
            hubJob.setOutputKeyClass(Text.class);
            hubJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(hubJob, authorityOutput);
            FileOutputFormat.setOutputPath(hubJob, hubOutput);
            
            if (!hubJob.waitForCompletion(true)) {
                System.exit(1);
            }
            
            // Этап 3: Нормализация (если требуется)
            if (useNormalization) {
                // Шаг 3.1: Вычисление суммы квадратов
                Path normSumOutput = new Path(outputPath + "/iteration-" + i + "-norm-sum");
                
                Job normSumJob = Job.getInstance(conf, "HITS Normalization Sum Iteration " + i);
                normSumJob.setJarByClass(HITSAlgorithm.class);
                normSumJob.setMapperClass(NormalizationSumMapper.class);
                normSumJob.setReducerClass(NormalizationSumReducer.class);
                normSumJob.setMapOutputKeyClass(Text.class);
                normSumJob.setMapOutputValueClass(DoubleWritable.class);
                normSumJob.setOutputKeyClass(Text.class);
                normSumJob.setOutputValueClass(DoubleWritable.class);
                FileInputFormat.addInputPath(normSumJob, hubOutput);
                FileOutputFormat.setOutputPath(normSumJob, normSumOutput);
                
                if (!normSumJob.waitForCompletion(true)) {
                    System.exit(1);
                }
                
                // Чтение нормализующих коэффициентов из файла
                double hubNormFactor = 1.0;
                double authNormFactor = 1.0;
                
                Path normSumFile = new Path(normSumOutput, "part-r-00000");
                if (fs.exists(normSumFile)) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(fs.open(normSumFile)))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split("\\s+");
                            if (parts.length >= 2) {
                                if (parts[0].equals("HUB_SQ_SUM")) {
                                    hubNormFactor = Double.parseDouble(parts[1]);
                                } else if (parts[0].equals("AUTH_SQ_SUM")) {
                                    authNormFactor = Double.parseDouble(parts[1]);
                                }
                            }
                        }
                    }
                }
                
                System.out.println("Normalization factors - HUB: " + hubNormFactor + 
                                   ", AUTH: " + authNormFactor);
                
                // Обновляем конфигурацию для следующего шага
                conf.setDouble("hub.norm.factor", hubNormFactor);
                conf.setDouble("auth.norm.factor", authNormFactor);
                
                // Шаг 3.2: Применение нормализации
                Path normApplyOutput = new Path(outputPath + "/iteration-" + i + "-norm-apply");
                
                Job normApplyJob = Job.getInstance(conf, "HITS Normalization Apply Iteration " + i);
                normApplyJob.setJarByClass(HITSAlgorithm.class);
                normApplyJob.setMapperClass(NormalizationApplyMapper.class);
                normApplyJob.setNumReduceTasks(0); // Нет reduce фазы, только map
                normApplyJob.setOutputKeyClass(Text.class);
                normApplyJob.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(normApplyJob, hubOutput);
                FileOutputFormat.setOutputPath(normApplyJob, normApplyOutput);
                
                if (!normApplyJob.waitForCompletion(true)) {
                    System.exit(1);
                }
                
                currentInput = normApplyOutput;
            } else {
                currentInput = hubOutput;
            }
        }
        
        // Копирование финального результата в основную выходную директорию
        Path finalOutput = new Path(outputPath + "/final");
        if (fs.exists(finalOutput)) {
            fs.delete(finalOutput, true);
        }
        fs.rename(currentInput, finalOutput);
        
        System.out.println("HITS algorithm completed successfully!");
        System.exit(0);
    }
}