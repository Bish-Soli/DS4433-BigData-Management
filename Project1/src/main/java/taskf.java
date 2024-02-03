import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class taskf {

    public static class FriendMapper extends Mapper<Object, Text, Text, Text> {
        private Text personId = new Text();
        private Text friendId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 2) {
                personId.set(parts[1]); // PersonID
                friendId.set("F:" + parts[2]); // MyFriend ID with "F" flag
                context.write(personId, friendId);
            }
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        private Text personId = new Text();
        private Text personName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 1) {
                personId.set(parts[0]); // PersonID
                personName.set("N:" + parts[1]); // Name with "N" flag
                context.write(personId, personName);
            }
        }
    }

    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        private Text user = new Text();
        private Text pageAccessed = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 2) {
                user.set(parts[1]); // ByWho
                pageAccessed.set("A:" + parts[2]); // WhatPage with "A" flag
                context.write(user, pageAccessed);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> nameMap = new HashMap<>();
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Skip the processing for header row or any row with 'PersonID' as the key
            if (key.toString().equals("PersonID")) {
                return;
            }

            Set<String> friends = new HashSet<>();
            Set<String> accessed = new HashSet<>();
            nameMap.clear();

            for (Text val : values) {
                String[] parts = val.toString().split(":", 2);
                if (parts[0].equals("F")) {
                    friends.add(parts[1]);
                } else if (parts[0].equals("A")) {
                    accessed.add(parts[1]);
                } else if (parts[0].equals("N")) {
                    // Also skip if the name is 'Name'
                    if (parts[1].equals("Name")) {
                        return;
                    }
                    nameMap.put(key.toString(), parts[1]);
                }
            }

            friends.removeAll(accessed); // Remove all friends that have been accessed

            // Only proceed if there are friends who haven't been accessed
            if (!friends.isEmpty()) {
                String name = nameMap.getOrDefault(key.toString(), "Unknown");
                result.set(name + "\t" + friends.size());
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Non Visiting Friend Finder");

        job.setJarByClass(taskf.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path("/user/project1/input/friends.csv"), TextInputFormat.class, FriendMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/access_logs.csv"), TextInputFormat.class, AccessLogMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/pages.csv"), TextInputFormat.class, PageMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("/user/project1/output/taskF_output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
