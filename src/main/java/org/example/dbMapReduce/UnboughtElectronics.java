package org.example.dbMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class UnboughtElectronics {

    public static class ProductsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 4) {
                String productId = parts[0].trim();
                String name = parts[1].trim();
                String category = parts[3].trim();

                // Фильтруем только товары категории Electronics
                if (category.equals("Electronics")) {
                    // Ключ - ID товара, значение - метка PRODUCT
                    context.write(new Text(productId), new Text("PRODUCT\t" + name));
                }
            }
        }
    }

    public static class OrdersMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String productId = parts[2].trim();
                // Ключ - ID товара, значение - метка ORDER
                context.write(new Text(productId), new Text("ORDER"));
            }
        }
    }

    public static class UnboughtReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isBought = false;
            String productName = null;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("PRODUCT")) {
                    productName = parts[1];
                } else if (parts[0].equals("ORDER")) {
                    isBought = true;
                }
            }

            // Если товар не был куплен, записываем его
            if (!isBought && productName != null) {
                context.write(new Text(productName), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: UnboughtElectronics <products_input> <orders_input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Unbought Electronics");
        job.setJarByClass(UnboughtElectronics.class);

        // Используем MultipleInputs для обработки двух разных входных файлов
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, ProductsMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, OrdersMapper.class);

        job.setReducerClass(UnboughtReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}