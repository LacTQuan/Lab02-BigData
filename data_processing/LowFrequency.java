import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LowFrequency {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text termId = new Text();
        private Text docId = new Text();
        private IntWritable frequency = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                termId.set(parts[0]);
                docId.set(parts[1].toString());
                frequency.set(Integer.parseInt(parts[2]));
                context.write(termId, new Text(docId + "\t" + frequency));
                // incompatible types: Text cannot be converted to IntWritable
                // context.write(termId, new Text(docId + "\t" + frequency));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> tempStorage = new ArrayList<>();
            int totalFrequency = 0;

            for (Text value : values) {
                tempStorage.add(value.toString());
                String[] components = value.toString().split("\t");
                int frequency = Integer.parseInt(components[1]);
                totalFrequency += frequency;
            }

            if (totalFrequency >= 3) {
                for (String item : tempStorage) {
                    String[] components = item.split("\t");
                    String docId = components[0].toString();
                    int frequency = Integer.parseInt(components[1]);

                    context.write(new Text(key.toString() + "\t" + docId), new IntWritable(frequency));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Filter low frequency terms");
        job.setJarByClass(LowFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path("/TermFrequency/data/output/task1_1.mtx"));
        FileOutputFormat.setOutputPath(job, new Path("/LowFrequency/data/output/"));
        FileSystem fs = FileSystem.get(conf);

        if (job.waitForCompletion(true)) {
            fs.rename(new Path("/LowFrequency/data/output/part-r-00000"), new Path("/LowFrequency/data/output/task1_2.mtx"));
        }
    }
}
