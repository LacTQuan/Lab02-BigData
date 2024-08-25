import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
    private List<String> stopWordsList = new ArrayList<>();
    private HashMap<String, Integer> termToIdMap = new HashMap<>();
    private HashMap<String, Integer> docToIdMap = new HashMap<>();
    private Configuration configuration;
    public static final String DOC_ID_PATH = "/TermFrequency/data/bbc/bbc.docs";
    public static final String TERM_ID_PATH = "/TermFrequency/data/bbc/bbc.terms";
    public static final String STOP_WORDS_PATH = "/TermFrequency/data/stopwords.txt";

    public Utils(Configuration configuration) {
        this.configuration = configuration;

        try {
            this.stopWordsList = this.loadStopWords(STOP_WORDS_PATH);
            this.docToIdMap = this.loadIds(DOC_ID_PATH);
            this.termToIdMap = this.loadIds(TERM_ID_PATH);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<String> loadStopWords(String filePath) throws IOException {
        List<String> stopWords = new ArrayList<>();
        FileSystem fileSystem = FileSystem.get(this.configuration);
        Path path = new Path(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        Throwable exception = null;

        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stopWords.add(line);
            }
        } catch (Throwable readException) {
            exception = readException;
            throw readException;
        } finally {
            if (bufferedReader != null) {
                if (exception != null) {
                    try {
                        bufferedReader.close();
                    } catch (Throwable closeException) {
                        exception.addSuppressed(closeException);
                    }
                } else {
                    bufferedReader.close();
                }
            }
        }

        return stopWords;
    }

    public HashMap<String, Integer> loadIds(String filePath) throws IOException {
        HashMap<String, Integer> idMap = new HashMap<>();
        FileSystem fileSystem = FileSystem.get(this.configuration);
        Path path = new Path(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        Throwable exception = null;

        try {
            String line;
            int idCounter = 1;
            while ((line = bufferedReader.readLine()) != null) {
                idMap.put(line, idCounter++);
            }
        } catch (Throwable readException) {
            exception = readException;
            throw readException;
        } finally {
            if (bufferedReader != null) {
                if (exception != null) {
                    try {
                        bufferedReader.close();
                    } catch (Throwable closeException) {
                        exception.addSuppressed(closeException);
                    }
                } else {
                    bufferedReader.close();
                }
            }
        }

        return idMap;
    }

    public boolean isStopWord(String word) {
        return this.stopWordsList.contains(word);
    }

    public int getTermId(String term) {
        return this.termToIdMap.getOrDefault(term, -1);
    }

    public int getDocId(String document) {
        return this.docToIdMap.getOrDefault(document, -1);
    }

    public String getTerm(int termId) {
        for (String term : this.termToIdMap.keySet()) {
            if (this.termToIdMap.get(term) == termId) {
                return term;
            }
        }
        return null;
    }

    public String getDoc(int docId) {
        for (String doc : this.docToIdMap.keySet()) {
            if (this.docToIdMap.get(doc) == docId) {
                return doc;
            }
        }
        return ".";
    }

    // Get number of documents per class
    public int getDocCount(String className) {
        int count = 0;
        for (String doc : this.docToIdMap.keySet()) {
            if (doc.split("\\.")[0].equals(className)) {
                count++;
            }
        }
        return count;
    }
}
