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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TfIdf {
    // TF
    public static class TfMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text docId = new Text();
        private Text termAndFreq = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            String termId = parts[0];
            String documentId = parts[1];
            String frequency = parts[2];

            docId.set(documentId);
            termAndFreq.set(termId + "\t" + frequency);

            context.write(docId, termAndFreq);
        }
    }

    public static class TfReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Key: docId, value: "termId frequency"
            int totalTermsInDoc = 0;
            Map<String, Integer> termFreqMap = new HashMap<>();

            for (Text val : values) {
                String[] termFreq = val.toString().split("\\s+");
                String termId = termFreq[0];
                int frequency = Integer.parseInt(termFreq[1]);
                totalTermsInDoc += frequency;
                termFreqMap.put(termId, frequency);
            }

            for (Map.Entry<String, Integer> entry : termFreqMap.entrySet()) {
                String termId = entry.getKey();
                double tf = (double) entry.getValue() / totalTermsInDoc;
                context.write(new Text(termId + "\t" + key.toString()), new Text(String.valueOf(tf)));
            }
        }
    }

    // IDF
    public static class IdfMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+"); // Input: "termId docId tfValue"
            String termId = parts[0];
            String docId = parts[1];
            String tfValue = parts[2];

            context.write(new Text(termId), new Text(docId + "\t" + tfValue));
        }
    }

    public static class TfIdfReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Double> docTfMap = new HashMap<>();
            int totalDocs = context.getConfiguration().getInt("totalDocs", 1);

            // Count the number of documents containing this term
            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                String docId = parts[0];
                double tf = Double.parseDouble(parts[1]);
                docTfMap.put(docId, tf);
            }

            // Calculate IDF
            double idf = Math.log((1.0 * totalDocs + 1) / (docTfMap.size() + 1)) + 1;
            // double idf = Math.log((1.0 * totalDocs) / (docTfMap.size()));

            // Calculate TF-IDF for each document and term
            for (Map.Entry<String, Double> entry : docTfMap.entrySet()) {
                String docId = entry.getKey();
                double tfIdf = entry.getValue() * idf;
                context.write(new Text(key.toString() + "\t" + docId), new Text(String.valueOf(tfIdf)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int totalDocs = 0;
        Configuration conf = new Configuration();

        // Count the number of documents
        FileSystem fs = FileSystem.get(conf);
        String inputDirectory = "/TermFrequency/data/test/";
        FileStatus[] folders = fs.globStatus(new Path(inputDirectory));
        // Get the number of files in the input directory
        for (FileStatus folder : folders) {
            FileStatus[] subfolders = fs.globStatus(new Path(folder.getPath().toString() + "/*"));
            for (FileStatus subfolder : subfolders) {
                FileStatus[] files = fs.globStatus(new Path(subfolder.getPath().toString() + "/*"));
                totalDocs += files.length;
            }
        }

        conf.setInt("totalDocs", totalDocs);

        // Job 1: Calculate TF
        Job job1 = Job.getInstance(conf, "TF Calculation");

        job1.setJarByClass(TfIdf.class);
        job1.setMapperClass(TfMapper.class);
        job1.setReducerClass(TfReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path("/LowFrequency/data/output/task1_2.mtx"));
        FileOutputFormat.setOutputPath(job1, new Path("/TfIdf/data/tmp/")); // Intermediate output

        boolean success = job1.waitForCompletion(true);

        if (!success) {
            System.exit(1);
        }

        // Job 2: Calculate TF-IDF
        Job job2 = Job.getInstance(conf, "TF-IDF Calculation");

        job2.setJarByClass(TfIdf.class);
        job2.setMapperClass(IdfMapper.class);
        job2.setReducerClass(TfIdfReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("/TfIdf/data/tmp/"));
        FileOutputFormat.setOutputPath(job2, new Path("/TfIdf/data/output/"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
