import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) throws Exception {
        // open input folder, read all files, and run TermFrequency on each file
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path("/TermFrequency/data/bbc-fulltext/"));
        // iterate over all folders
        for (FileStatus fileStatus : status) {
            if (fileStatus.isDirectory()) {
                String folderPath = fileStatus.getPath().toString();
                String folderName = folderPath.substring(folderPath.lastIndexOf("/") + 1);
                FileStatus[] files = fs.listStatus(fileStatus.getPath());
                // iterate over all files in the folder
                for (FileStatus file : files) {
                    String filePath = file.getPath().toString();
                    String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
                    String docName = folderName + "." + fileName;
                    String inputPath = filePath;
                    String outputPath = "/TermFrequency/data/output/" + docName;
                    TermFrequency termFrequency = new TermFrequency(docName, inputPath, outputPath);
                    termFrequency.run();
                }
            }
        }
    }
}
