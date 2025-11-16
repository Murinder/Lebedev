package org.example.dbMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ElectronicsOrdersStep2 {

    public static class CustomersJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, String> customersMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String customersPath = conf.get("customers.path");

            // Загружаем данные о клиентах в память
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(customersPath))));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    String customerId = parts[0].trim();
                    String name = parts[1].trim();
                    customersMap.put(customerId, name);
                }
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String orderId = parts[0];
                String[] orderDetails = parts[1].split(",");

                if (orderDetails.length >= 4) {
                    String customerId = orderDetails[0];
                    String productName = orderDetails[1];
                    String quantity = orderDetails[2];
                    String orderDate = orderDetails[3];

                    // Находим имя клиента
                    if (customersMap.containsKey(customerId)) {
                        String customerName = customersMap.get(customerId);
                        String result = customerName + "," + productName + "," + quantity;
                        context.write(new Text(customerName), new Text(result));
                    }
                }
            }
        }
    }

    public static class CustomersJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: ElectronicsOrdersStep2 <step1_output> <customers_input> <output>");
            System.exit(2);
        }

        conf.set("customers.path", otherArgs[1]);

        Job job = Job.getInstance(conf, "Electronics Orders Step 2");
        job.setJarByClass(ElectronicsOrdersStep2.class);
        job.setMapperClass(CustomersJoinMapper.class);
        job.setReducerClass(CustomersJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}