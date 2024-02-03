import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class taskb {

    public static class AccessLogMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static Text one = new Text("1");
        private IntWritable pageId = new IntWritable();
        private boolean skipHeader = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (skipHeader) {
                skipHeader = false;
                return;
            }

            String[] parts = value.toString().split(",");
            if (parts.length > 2) {
                try {
                    pageId.set(Integer.parseInt(parts[2])); // WhatPage as the key
                    context.write(pageId, one); // Emit count of 1 for each access as Text
                } catch (NumberFormatException e) {
                    // Log and skip if parsing fails
                    context.getCounter("Mapper", "InvalidNumbers").increment(1);
                }
            }
        }
    }

    public static class PageDetailsMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable pageId = new IntWritable();
        private Text details = new Text();
        private boolean skipHeader = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (skipHeader) {
                // Skip the header row
                skipHeader = false;
                return;
            }

            String[] parts = value.toString().split(",");
            if (parts.length > 2) {
                try {
                    pageId.set(Integer.parseInt(parts[0])); // ID as the key
                    details.set("details:" + parts[1] + "," + parts[2]); // Prepend "details:" to mark this as details
                    context.write(pageId, details);
                } catch (NumberFormatException e) {
                    // Log and skip if parsing fails
                    context.getCounter("Mapper", "InvalidNumbers").increment(1);
                }
            }
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        private TreeMap<Integer, List<String>> topPages = new TreeMap<>(Collections.reverseOrder());

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            List<String> detailsList = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(":", 2);
                if (parts[0].equals("details")) {
                    detailsList.add(key.toString() + "," + parts[1]);
                } else {
                    sum += Integer.parseInt(parts[0]);
                }
            }

            if (sum > 0 && !detailsList.isEmpty()) {
                if (!topPages.containsKey(sum)) {
                    topPages.put(sum, new ArrayList<>());
                }
                topPages.get(sum).addAll(detailsList);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Map.Entry<Integer, List<String>> entry : topPages.entrySet()) {
                for (String details : entry.getValue()) {
                    if (count++ < 10) {
                        context.write(NullWritable.get(), new Text(entry.getKey() + "\t" + details));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Accessed Pages");

        job.setJarByClass(taskb.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("/user/project1/input/access_logs.csv"), TextInputFormat.class, AccessLogMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/pages.csv"), TextInputFormat.class, PageDetailsMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("/user/project1/output/taskB_output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
