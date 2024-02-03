import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class taska {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text nameAndHobby = new Text();
        private Text nationality = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split CSV line
            String[] parts = value.toString().split(",");
            // Check if the array has at least 5 elements to prevent index out of bounds
            if (parts.length > 4) {
                String personNationality = parts[2].trim();
                // If nationality matches "Latvia", write it to the context
                if ("Latvia".equals(personNationality)) {
                    String name = parts[1].trim();
                    String hobby = parts[4].trim();
                    nameAndHobby.set(name + ", " + hobby);
                    nationality.set(personNationality);
                    context.write(nationality, nameAndHobby);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text val : values) {
                stringBuilder.append(val.toString());
                stringBuilder.append("\n");
            }
            result.set(stringBuilder.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality hobby report");
        job.setJarByClass(taska.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/user/project1/input/pages.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/user/project1/output/taskA_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
