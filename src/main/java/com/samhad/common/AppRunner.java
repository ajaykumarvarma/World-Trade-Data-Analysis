package com.samhad.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class AppRunner {

    public int jobRunner(Configuration configuration, String[] args, Class<?> outputKeyClass,
                         Class<?> appClass, Class<?> reducerClass,
                         Class<?> importMapperClass, Class<?> exportMapperClass)
            throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(configuration, "India Import Export Join Job");

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: hadoop jar IndianTradeData.jar <com.samhad.app.CYApp/com.samhad.app.CYCApp> </import-data> </export-data> </output-path>");
            return 2;
        }

        job.setJarByClass(appClass);

        job.setMapOutputKeyClass(outputKeyClass);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, (Class<? extends Mapper>) importMapperClass);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, (Class<? extends Mapper>) exportMapperClass);

        job.setReducerClass((Class<? extends Reducer>) reducerClass);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
