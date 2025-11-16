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

public class ElectronicsOrdersStep1 {

    public static class ElectronicsJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, String> productsMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String productsPath = conf.get("products.path");

            // Загружаем данные о товарах в память
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(productsPath))));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 4) {
                    String productId = parts[0].trim();
                    String name = parts[1].trim();
                    String category = parts[3].trim();

                    // Сохраняем только товары категории Electronics
                    if (category.equals("Electronics")) {
                        productsMap.put(productId, name);
                    }
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

                // Если товар из категории Electronics, сохраняем информацию о заказе
                if (productsMap.containsKey(productId)) {
                    String productName = productsMap.get(productId);
                    String result = customerId + "," + productName + "," + quantity + "," + orderDate;
                    context.write(new Text(orderId), new Text(result));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: ElectronicsOrdersStep1 <orders_input> <products_input> <output>");
            System.exit(2);
        }

        conf.set("products.path", otherArgs[1]);

        Job job = Job.getInstance(conf, "Electronics Orders Step 1");
        job.setJarByClass(ElectronicsOrdersStep1.class);
        job.setMapperClass(ElectronicsJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}