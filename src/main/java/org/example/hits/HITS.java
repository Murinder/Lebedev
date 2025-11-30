package org.example.hits;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class HITS {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: HITS <input_path> <output_path> <num_iterations>");
            System.exit(2);
        }

        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];
        int numIterations = Integer.parseInt(otherArgs[2]);

        FileSystem fs = FileSystem.get(conf);

        //Удаляем выходную директорию, если она существует
        Path outputDir = new Path(outputPath);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        //Запуск итераций алгоритма HITS
        String currentInput = inputPath;
        for (int i = 1; i <= numIterations; i++) {
            System.out.println("Starting iteration " + i + " of " + numIterations);

            //Обновление оценок авторитетности
            Path authUpdateOutput = new Path(outputPath + "/iter_" + i + "/auth_update");
            runJob(conf, currentInput, authUpdateOutput.toString(),
                    AuthorityUpdateMapper.class, AuthorityUpdateReducer.class,
                    Text.class, Text.class, Text.class, Text.class);

            //Вычисление суммы квадратов оценок авторитетности
            Path authSumOutput = new Path(outputPath + "/iter_" + i + "/auth_sum");
            runJob(conf, authUpdateOutput.toString(), authSumOutput.toString(),
                    AuthoritySumMapper.class, AuthoritySumReducer.class,
                    Text.class, Text.class, Text.class, Text.class);

            //Получение нормализующего множителя для оценок авторитетности
            double authNormFactor = getNormalizationFactor(conf, authSumOutput.toString(), "auth_sum");
            System.out.println("Authority normalization factor: " + authNormFactor);

            //Нормализация оценок авторитетности
            Path authNormOutput = new Path(outputPath + "/iter_" + i + "/auth_norm");
            runJobWithConfig(conf, authUpdateOutput.toString(), authNormOutput.toString(),
                    AuthorityNormalizationMapper.class, null,
                    Text.class, Text.class, Text.class, Text.class,
                    "auth.norm.factor", authNormFactor);

            //Обновление посреднических оценок
            Path hubUpdateOutput = new Path(outputPath + "/iter_" + i + "/hub_update");
            runJob(conf, authNormOutput.toString(), hubUpdateOutput.toString(),
                    HubUpdateMapper.class, HubUpdateReducer.class,
                    Text.class, Text.class, Text.class, Text.class);

            //Вычисление суммы квадратов посреднических оценок
            Path hubSumOutput = new Path(outputPath + "/iter_" + i + "/hub_sum");
            runJob(conf, hubUpdateOutput.toString(), hubSumOutput.toString(),
                    HubSumMapper.class, HubSumReducer.class,
                    Text.class, Text.class, Text.class, Text.class);

            //Получение нормализующего множителя для посреднических оценок
            double hubNormFactor = getNormalizationFactor(conf, hubSumOutput.toString(), "hub_sum");
            System.out.println("Hub normalization factor: " + hubNormFactor);

            //Нормализация посреднических оценок
            Path hubNormOutput = new Path(outputPath + "/iter_" + i + "/hub_norm");
            runJobWithConfig(conf, hubUpdateOutput.toString(), hubNormOutput.toString(),
                    HubNormalizationMapper.class, null,
                    Text.class, Text.class, Text.class, Text.class,
                    "hub.norm.factor", hubNormFactor);

            //Установка выходного пути текущей итерации как входного пути для следующей итерации
            currentInput = hubNormOutput.toString();

            System.out.println("Completed iteration " + i);
        }

        System.out.println("HITS algorithm completed successfully!");
    }

    private static void runJob(Configuration conf, String inputPath, String outputPath,
                               Class<? extends Mapper> mapperClass,
                               Class<? extends Reducer> reducerClass,
                               Class<? extends WritableComparable> outputKeyClass,
                               Class<? extends Writable> outputValueClass,
                               Class<? extends WritableComparable> mapOutputKeyClass,
                               Class<? extends Writable> mapOutputValueClass) throws Exception {

        Job job = Job.getInstance(conf, "HITS Job");
        job.setJarByClass(HITS.class);
        job.setMapperClass(mapperClass);

        if (reducerClass != null) {
            job.setReducerClass(reducerClass);
        } else {
            job.setNumReduceTasks(0);
        }

        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new RuntimeException("Job failed: " + job.getJobName());
        }
    }

    private static void runJobWithConfig(Configuration conf, String inputPath, String outputPath,
                                         Class<? extends Mapper> mapperClass,
                                         Class<? extends Reducer> reducerClass,
                                         Class<? extends WritableComparable> outputKeyClass,
                                         Class<? extends Writable> outputValueClass,
                                         Class<? extends WritableComparable> mapOutputKeyClass,
                                         Class<? extends Writable> mapOutputValueClass,
                                         String configKey, double configValue) throws Exception {

        Configuration jobConf = new Configuration(conf);
        jobConf.setDouble(configKey, configValue);
        runJob(jobConf, inputPath, outputPath, mapperClass, reducerClass,
                outputKeyClass, outputValueClass, mapOutputKeyClass, mapOutputValueClass);
    }

    private static double getNormalizationFactor(Configuration conf, String path, String key) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(path + "/part-r-00000");

        if (!fs.exists(filePath)) {
            System.err.println("Warning: Output file not found: " + filePath);
            return 1.0; //Возвращаем 1.0 как безопасное значение по умолчанию
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length >= 2 && parts[0].equals(key)) {
                reader.close();
                return Double.parseDouble(parts[1]);
            }
        }

        reader.close();
        System.err.println("Warning: Normalization factor not found for key: " + key);
        return 1.0; //Возвращаем 1.0 как безопасное значение по умолчанию
    }
}