package com.company;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCounter {

    public static class TokenizerMapper

            extends Mapper < Object, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text text, Context context) throws IOException,
                InterruptedException {

            if (text == null || text.isEmpty())
                return;

            StringTokenizer st  = new StringTokenizer(text.toString());
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer < Text, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable < IntWritable > values,
                           Context context
        ) throws IOException,
                InterruptedException {
            int total = 0;
            for (IntWritable val: values) {
                total += val.get();
            }
            result.set(total);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word counter");
        job.setJarByClass(WordCounter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // conf.set("fs.defaultFS", "hdfs://localhost:9000");  // where key="fs.default.name"|"fs.defaultFS"

        // FileSystem hdfs = FileSystem.get(conf);
        // Path workingDir=hdfs.getWorkingDirectory();

        // Path input= new Path("/input");
        // input=Path.mergePaths(workingDir, input);
        // if(hdfs.exists(input))
        // {
        //     hdfs.delete(input); //Delete existing Directory
        // }
        // FileInputFormat.addInputPath(job, input);


        // Path outputDirPath= new Path("/output");
        // outputDirPath=Path.mergePaths(workingDir, outputDirPath);
        // if(hdfs.exists(outputDirPath))
        // {
        //     hdfs.delete(outputDirPath); //Delete existing Directory
        // }

        // FileOutputFormat.setOutputPath(job, outputDirPath);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
