package com.wm.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * created Khanh
 * at Dec 10 2019
 */
public class ImageUrlCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                String url = extractUrl(line);
                if (!url.isEmpty()) {
                   // System.out.println(url);
                    word.set(url);
                    context.write(word, one);
                }
            }
        }
    }

    private static String extractUrl(String line) {
        try {
            String pattern = "GET /asset-store-read/api/asr/";
            int index0 = line.indexOf(pattern);
            if (index0 > 0) {
                int index1 = line.indexOf('?', index0);
                if (index1 > 0) {
                    return line.substring(index0 + pattern.length(), index1);
                }
            }
        } catch (Exception e) {
            System.err.println(line);
            e.printStackTrace();
        }
        return "";
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //args = new String[]{"/Users/d0l0278/tmp_log/20191210","/Users/d0l0278/tmp_log/20191210/output"};
        if (args == null || args.length < 2) throw new IllegalArgumentException("Invalid argument paths");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "url count");
        job.setJarByClass(ImageUrlCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.out.println("Job Map Reduce Count URL Done");
        }

        System.out.println("Summary Save : ");
        SummaryOutput sOut = new SummaryOutput();
        sOut.run(args[1]);
        System.exit(  1);

    }
}