import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class taskh {

    public static class FriendsMapper extends Mapper<Object, Text, Text, Text> {
        private Text personID = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header line
            if (value.toString().contains("FriendRel")) {
                return;
            }

            String[] parts = value.toString().split(",");
            if (parts.length > 2) {
                personID.set(parts[1].trim()); // PersonID as key
                outputValue.set("F,1"); // Prepend 'F' to denote friends dataset
                context.write(personID, outputValue);
            }
        }
    }

    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        private Text personID = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header line
            if (value.toString().contains("PersonID")) {
                return;
            }

            String[] parts = value.toString().split(",");
            if (parts.length > 1) {
                personID.set(parts[0].trim()); // PersonID as key
                outputValue.set("P," + parts[1]); // Prepend 'P' to denote pages dataset
                context.write(personID, outputValue);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
        private Map<String, String> namesMap = new HashMap<>();
        private Map<String, Integer> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String personId = key.toString();
            int sum = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("P")) {
                    namesMap.put(personId, parts[1]);
                } else {
                    sum += Integer.parseInt(parts[1]);
                }
            }
            countMap.put(personId, sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double totalFriends = 0;
            for (int count : countMap.values()) {
                totalFriends += count;
            }
            double average = totalFriends / countMap.size();

            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                if (entry.getValue() > average) {
                    context.write(new Text(namesMap.get(entry.getKey())), new IntWritable(entry.getValue()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "popular users join");
        job.setJarByClass(taskh.class);

        // Set the reducer class
        job.setReducerClass(JoinReducer.class);

        // Set the output key and value classes for the entire job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the map output key and value classes for the entire job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Use MultipleInputs to set which mapper to use for each input file
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/friends.csv"), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/pages.csv"), TextInputFormat.class, PagesMapper.class);

        // Set the output path
        FileOutputFormat.setOutputPath(job, new Path("/user/project1/output/taskH_output"));

        // Run the job and wait for it to be completed
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
