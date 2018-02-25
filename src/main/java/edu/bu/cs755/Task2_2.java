package edu.bu.cs755;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

public class Task2_2 {

    public static class SortMedallionErrors extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        PriorityQueue<Map.Entry<Text, DoubleWritable>> q = new PriorityQueue<>(6, new Comparator<Map.Entry<Text, DoubleWritable>>() {
            @Override
            public int compare(Map.Entry<Text, DoubleWritable> e1, Map.Entry<Text, DoubleWritable> e2) {
                return e1.getValue().compareTo(e2.getValue());
            }
        });

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            Text medallion = new Text(fields[0]);
            DoubleWritable percentage = new DoubleWritable(Double.parseDouble(fields[1]));

            // Add the map the q
            Map <Text, DoubleWritable> map = new HashMap();
            map.put(medallion, percentage);
            q.addAll(map.entrySet());

            // If the q is greater than 5, remove the least
            if (q.size() >= 6) {
                q.poll();
            }
        }

        public void cleanup(Context context
        ) throws IOException, InterruptedException {
            while(!q.isEmpty()) {
                context.write(q.peek().getValue(), q.peek().getKey());
                q.remove();
            }
        }
    }

    public static class MedallionErrorRateReducer extends Reducer<DoubleWritable,Text,DoubleWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "task2.1");
        job.setJarByClass(Task2_2.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Task2_2.SortMedallionErrors.class);
        job.setReducerClass(Task2_2.MedallionErrorRateReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}