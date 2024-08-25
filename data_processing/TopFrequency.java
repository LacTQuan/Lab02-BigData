import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TopFrequency {
    // Calculate the frequency of each term
    public static class TermFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text termId = new Text();
        private IntWritable frequency = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                termId.set(parts[0]);
                frequency.set(Integer.parseInt(parts[2]));
                context.write(termId, frequency);
            }
        }
    }

    public static class TermFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Sort the terms by frequency and output the top 10 terms
    public static class TopTermsMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable frequency = new IntWritable();
        private Text termId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                termId.set(parts[0]);
                frequency.set(Integer.parseInt(parts[1]));
                context.write(termId, frequency);
            }
        }
    }

    public static class TopTermsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private PriorityQueue<TermFrequencyPair> topTerms = new PriorityQueue<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                topTerms.add(new TermFrequencyPair(key.toString(), value.get()));
                if (topTerms.size() > 10) {
                    topTerms.poll();
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!topTerms.isEmpty()) {
                TermFrequencyPair pair = topTerms.poll();
                context.write(new Text(pair.getTerm()), new IntWritable(pair.getFrequency()));
            }
        }

        private class TermFrequencyPair implements Comparable<TermFrequencyPair> {
            private String term;
            private int frequency;

            public TermFrequencyPair(String term, int frequency) {
                this.term = term;
                this.frequency = frequency;
            }

            public String getTerm() {
                return term;
            }

            public int getFrequency() {
                return frequency;
            }

            @Override
            public int compareTo(TermFrequencyPair o) {
                return Integer.compare(this.frequency, o.frequency);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Utils utils = new Utils(conf);
        Job job = Job.getInstance(conf, "top frequency terms");
        job.setJarByClass(TopFrequency.class);
        job.setMapperClass(TermFrequencyMapper.class);
        job.setCombinerClass(TermFrequencyReducer.class);
        job.setReducerClass(TermFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/LowFrequency/data/output/task1_2.mtx"));
        FileOutputFormat.setOutputPath(job, new Path("/TopFrequency/data/tmp/"));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "top terms");
        job2.setJarByClass(TopFrequency.class);
        job2.setMapperClass(TopTermsMapper.class);
        job2.setCombinerClass(TopTermsReducer.class);
        job2.setReducerClass(TopTermsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("/TopFrequency/data/tmp/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path("/TopFrequency/data/output/"));
        job2.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        fs.rename(new Path("/TopFrequency/data/output/part-r-00000"), new Path("/TopFrequency/data/output/task1_3.mtx"));

        // Match term ids with term names and save to task1_3.txt
        FileStatus[] status = fs.listStatus(new Path("/TopFrequency/data/output/"));
        try(BufferedWriter writer = new BufferedWriter(new FileWriter("/TopFrequency/data/task1_3.txt"))) {
            for (FileStatus file : status) {
                try(BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\\s+");
                        writer.write(utils.getTerm(Integer.parseInt(parts[0])) + " " + parts[1] + "\n");
                    }
                }
            }
        }
    }
}
