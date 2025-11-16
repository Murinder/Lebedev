package org.example.dbMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class AllOrdersStep1 {

    public static class OrdersJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
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
            String[] parts = value.toString().split(",");
            if (parts.length >= 5) {
                String orderId = parts[0].trim();
                String customerId = parts[1].trim();
                String productId = parts[2].trim();
                String orderDate = parts[3].trim();
                String quantity = parts[4].trim();

                // Если клиент найден, сохраняем информацию о заказе
                if (customersMap.containsKey(customerId)) {
                    String customerName = customersMap.get(customerId);
                    String result = customerName + "," + productId + "," + orderDate + "," + quantity;
                    context.write(new Text(orderId), new Text(result));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: AllOrdersStep1 <orders_input> <customers_input> <output>");
            System.exit(2);
        }

        conf.set("customers.path", otherArgs[1]);

        Job job = Job.getInstance(conf, "All Orders Step 1");
        job.setJarByClass(AllOrdersStep1.class);
        job.setMapperClass(OrdersJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}