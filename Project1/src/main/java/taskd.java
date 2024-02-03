import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class taskd {


    public static class FriendMapper extends Mapper<Object, Text, Text, Text> {
        private Text friendId = new Text();
        private Text countValue = new Text("1");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Check if this line is the header
            if (line.contains("FriendRel") || line.contains("MyFriend")) {
                // Skip this line
                return;
            }
            String[] parts = line.split(",");
            friendId.set(parts[2].trim()); // MyFriend ID
            context.write(friendId, countValue);
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        private Text personId = new Text();
        private Text personName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Check if this line is the header
            if (line.contains("PersonID") || line.contains("Name")) {
                // Skip this line
                return;
            }
            String[] parts = line.split(",");
            personId.set(parts[0].trim()); // PersonID
            personName.set("NAME:" + parts[1].trim()); // Prefix the name with "NAME:"
            context.write(personId, personName);
        }
    }

    public static class ConnectednessReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String name = "";
            for (Text val : values) {
                if (val.toString().startsWith("NAME:")) {
                    name = val.toString().substring(5); // Extract the name
                } else {
                    // Since the count is emitted as Text "1", we can simply add it
                    sum += Integer.parseInt(val.toString());
                }
            }
            if (!name.isEmpty()) {
                context.write(new Text(name), new IntWritable(sum)); // Output the name and connectedness factor
            } else {
                context.write(key, new IntWritable(0)); // If no name, output the ID with zero
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Connectedness Factor");
        job.setJarByClass(taskd.class);
        job.setReducerClass(ConnectednessReducer.class);
        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); // Changed to Text because the output will be Text
        // Add two input paths with their respective mappers
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/friends.csv"), TextInputFormat.class, FriendMapper.class); // args[0] should be the path to the Friends dataset
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/pages.csv"), TextInputFormat.class, PageMapper.class);   // args[1] should be the path to the MyPage dataset

        // Set the path for the job output
        FileOutputFormat.setOutputPath(job, new Path("/user/project1/output/taskd_output")); // args[2] should be the path for the output directory

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
