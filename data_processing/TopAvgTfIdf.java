import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;
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

public class TopAvgTfIdf {
    private static Configuration conf = new Configuration();
    private static Utils utils = new Utils(TopAvgTfIdf.conf);

    public static class TfidfMapper extends Mapper<Object, Text, Text, Text> {
        private Text classKey = new Text();
        private Text termValue = new Text();
        private static final Logger logger = Logger.getLogger(TfidfMapper.class);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            int termId = Integer.parseInt(parts[0]);
            int docId = Integer.parseInt(parts[1]);
            double tfidfScore = Double.parseDouble(parts[2]);

            String term = TopAvgTfIdf.utils.getTerm(termId);
            String className = TopAvgTfIdf.utils.getDoc(docId);
            className = className.substring(0, className.indexOf('.'));

            classKey.set(className);
            termValue.set(term + "\t" + tfidfScore);
            context.write(classKey, termValue);
        }
    }

    public static class AvgTfidfReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> termTfidfMap = new HashMap<>();

            for (Text val : values) {
                String[] termTfidf = val.toString().split("\\s+");
                String term = termTfidf[0];
                double tfidfScore = Double.parseDouble(termTfidf[1]);

                termTfidfMap.put(term, termTfidfMap.getOrDefault(term, 0.0) + tfidfScore);
            }

            for (Map.Entry<String, Double> entry : termTfidfMap.entrySet()) {
                String term = entry.getKey();
                double totalTfidf = entry.getValue();
                int numDocs = TopAvgTfIdf.utils.getDocCount(key.toString());
                double avgTfidf = (1.0 * totalTfidf) / numDocs;

                context.write(key, new Text(term + "\t" + avgTfidf));
            }
        }
    }

    public static class TopTermsMapper extends Mapper<Object, Text, Text, Text> {
        private Text classKey = new Text();
        private Text termValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            String className = parts[0];
            String term = parts[1];
            String avgTfidf = parts[2];

            classKey.set(className);
            termValue.set(term + "\t" + avgTfidf);
            context.write(classKey, termValue);
        }
    }

    public static class TopTermsReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            PriorityQueue<Map.Entry<String, Double>> pq = new PriorityQueue<>(
                    Comparator.comparingDouble(Map.Entry::getValue));

            for (Text val : values) {
                String[] termTfidf = val.toString().split("\\s+");
                String term = termTfidf[0];
                double avgTfidf = Double.parseDouble(termTfidf[1]);

                pq.offer(new AbstractMap.SimpleEntry<>(term, avgTfidf));

                if (pq.size() > 5) {
                    pq.poll();
                }
            }

            List<Map.Entry<String, Double>> topTerms = new ArrayList<>();
            while (!pq.isEmpty()) {
                topTerms.add(pq.poll());
            }

            Collections.reverse(topTerms);

            StringBuilder topTermsStr = new StringBuilder();
            for (Map.Entry<String, Double> entry : topTerms) {
                if (topTermsStr.length() > 0) {
                    topTermsStr.append(", ");
                }
                topTermsStr.append(entry.getKey()).append(":").append(entry.getValue());
            }

            context.write(key, new Text(topTermsStr.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(TopAvgTfIdf.conf, "top average tfidf");
        job.setJarByClass(TopAvgTfIdf.class);
        job.setMapperClass(TfidfMapper.class);
        // job.setCombinerClass(TermFrequencyReducer.class);
        job.setReducerClass(AvgTfidfReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/TfIdf/data/output/task1_4.mtx"));
        FileOutputFormat.setOutputPath(job, new Path("/TopAvgTfIdf/data/tmp/"));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "top terms");
        job2.setJarByClass(TopAvgTfIdf.class);
        job2.setMapperClass(TopTermsMapper.class);
        // job2.setCombinerClass(TopTermsReducer.class);
        job2.setReducerClass(TopTermsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("/TopAvgTfIdf/data/tmp/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path("/TopAvgTfIdf/data/output/"));
        job2.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        fs.rename(new Path("/TopAvgTfIdf/data/output/part-r-00000"),
                new Path("/TopAvgTfIdf/data/output/task1_5.txt"));
    }
}
