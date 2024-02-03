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
import java.util.HashSet;

public class taske {

    // Mapper that emits the user ID and the accessed page ID
    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        private Text byWho = new Text();
        private Text whatPage = new Text("PAGE:");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            byWho.set(parts[1]); // ByWho
            whatPage.set("PAGE:" + parts[2]); // Prefix with PAGE: to indicate it's a page access
            context.write(byWho, whatPage);
        }
    }

    // Page Mapper
    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text userName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            userId.set(parts[0]); // PersonID
            userName.set("NAME:" + parts[1]); // Prefix with NAME: to indicate it's a user name
            context.write(userId, userName);
        }
    }

    // Reducer
    public static class FavoritesReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccesses = 0;
            HashSet<String> uniquePages = new HashSet<>();
            String name = null; // Initialize name as null

            for (Text value : values) {
                String valStr = value.toString();
                if (valStr.startsWith("PAGE:")) {
                    totalAccesses++;
                    uniquePages.add(valStr.substring(5)); // Remove prefix
                } else if (valStr.startsWith("NAME:")) {
                    name = valStr.substring(5); // Extract the name
                }
            }

            if (name != null && totalAccesses > 0) {
                // Construct the output string without the redundant name key
                result.set(Integer.toString(totalAccesses) + "\t" + Integer.toString(uniquePages.size()));
                context.write(new Text(name), result); // Use name as the key
            }
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Determine Favorites");

        job.setJarByClass(taske.class);
        job.setReducerClass(FavoritesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set multiple inputs with respective mappers
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/access_logs.csv"), TextInputFormat.class, AccessLogMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/pages.csv"), TextInputFormat.class, PageMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("/user/project1/output/taskE_output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}