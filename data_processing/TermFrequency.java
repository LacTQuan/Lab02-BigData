import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

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

public class TermFrequency {
    private static Configuration conf = new Configuration();
    private static Utils utils = new Utils(TermFrequency.conf);
    private static final Pattern SPECIAL_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9]");

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Get input split information
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String[] pathNames = fileSplit.getPath().getParent().toString().split("/");
            String folderName = pathNames[pathNames.length - 1];
            String fileName = fileSplit.getPath().getName().split("\\.")[0];

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String term = itr.nextToken();
                term = SPECIAL_CHAR_PATTERN.matcher(term).replaceAll("");
                if (!TermFrequency.utils.isStopWord(term)) {
                    int termId = TermFrequency.utils.getTermId(term);
                    int docId = TermFrequency.utils.getDocId(folderName + "." + fileName);

                    if (termId != -1 && docId != -1) {
                        word.set(termId + "\t" + docId);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(TermFrequency.conf, "word count");
        job.setJarByClass(TermFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(TermFrequency.conf);

        FileStatus[] folders = fs.globStatus(new Path("/TermFrequency/data/test/"));
        // for (int k = 0; k < folders.length; k++) {
        //     if (subfolders[k].isDirectory()) {
        //         FileInputFormat.addInputPath(job, new Path(subfolders[k].getPath().toString() + "/*"));
        //     }
        // }

        for (FileStatus folder : folders) {
            FileStatus[] subfolders = fs.globStatus(new Path(folder.getPath().toString() + "/*"));
            for (FileStatus subfolder : subfolders) {
                FileInputFormat.addInputPath(job, new Path(subfolder.getPath().toString() + "/*"));
            }
        }
        String outputPath = "/TermFrequency/data/output/";
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (job.waitForCompletion(true)) {
            // Path outputPath = new Path(this.outputPath);
            // FileSystem fs = FileSystem.get(TermFrequency.conf);
            // FileStatus[] status = fs.listStatus(outputPath);
            // for (FileStatus fileStatus : status) {
            //     Path filePath = fileStatus.getPath();
            //     if (!filePath.getName().startsWith("_")) { // Skip Hadoop temporary files
            //         try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
            //                 BufferedWriter bw = new BufferedWriter(new FileWriter("output.mtx", true))) {

            //             String line;
            //             while ((line = br.readLine()) != null) {
            //                 String[] parts = line.split("\\s+");
            //                 // String[] keyParts = parts[0].split(":");
            //                 String term = parts[0];
            //                 String termId = String.valueOf(TermFrequency.utils.getTermId(term));
            //                 int count = Integer.parseInt(parts[1]);

            //                 // Write to Matrix Market format: termId docId count
            //                 bw.write(termId + " "  + term + " " + docId + " " + count);
            //                 bw.newLine();
            //             }
            //         }
            //     }
            // }

            fs.rename(new Path(outputPath + "/part-r-00000"), new Path(outputPath + "/task1_1.mtx"));
            System.exit(0);
        }
    }
}
