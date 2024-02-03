import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class taskg {


    private static final String ACCESS_LOG_RECORD = "ALOG";
    private static final String PAGE_RECORD = "PAGE";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text userDetails = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 2) {
                userId.set(parts[0]); // PersonID
                userDetails.set(PAGE_RECORD + "," + parts[1]); // Tag record type and Name
                context.write(userId, userDetails);
            }
        }
    }


    public static class LastAccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private final static SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 1) {
                userId.set(parts[1].trim()); // ByWho
                try {
                    Date date = dateFormat.parse(parts[4].trim()); // AccessTime
                    context.write(userId, new Text(dateFormat.format(date))); // Emit as Text
                } catch (Exception e) {
                    // Handle parse exception for date
                    context.write(userId, new Text("0")); // Indicates no access
                }
            }
        }
    }

    public static class LastAccessReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private long fourteenDaysAgo;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Calculate fourteenDaysAgo in milliseconds
            fourteenDaysAgo = System.currentTimeMillis() - (14L * 24 * 60 * 60 * 1000);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long mostRecentAccess = 0; // Use 0 or a suitable value to indicate no access
            String userName = null;

            for (Text val : values) {
                String[] parts = val.toString().split(",", 2);
                if (parts[0].equals(ACCESS_LOG_RECORD)) {
                    try {
                        Date accessTime = dateFormat.parse(parts[1]);
                        mostRecentAccess = Math.max(mostRecentAccess, accessTime.getTime());
                    } catch (Exception e) {
                        // Handle parse exception for date
                    }
                } else if (parts[0].equals(PAGE_RECORD)) {
                    userName = parts[1];
                }
            }

            if (userName != null && mostRecentAccess < fourteenDaysAgo) {
                result.set(userName);
                context.write(key, result); // Emit PersonID and userName
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Disconnected Users");

        job.setJarByClass(taskg.class);
        job.setMapperClass(LastAccessMapper.class);
        job.setReducerClass(LastAccessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path("/user/project1/input/access_logs.csv"), TextInputFormat.class, LastAccessMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/project1/input/pages.csv"), TextInputFormat.class, PageMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("/user/project1/output/taskG_output")); // Path for output

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
