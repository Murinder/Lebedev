package org.example.crossCorrelation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrossCorrelationDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: <input> <output_pairs> <output_stripes>");
            System.exit(1);
        }

        // === Pairs Job ===
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "CrossCorrelation Pairs");
        job1.setJarByClass(CrossCorrelationDriver.class);
        job1.setMapperClass(PairsMapper.class);
        job1.setCombinerClass(PairsReducer.class);
        job1.setReducerClass(PairsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // === Stripes Job ===
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "CrossCorrelation Stripes");
        job2.setJarByClass(CrossCorrelationDriver.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setMapperClass(StripesMapper.class);
        job2.setReducerClass(StripesReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
    }
}