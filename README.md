# Lab02-BigData

## Running scripts

```
cd data_processing
```

## Term Frequency
```
hadoop fs -mkdir -p /TermFrequency/output/
javac -classpath $(hadoop classpath) -d . Utils.java TermFrequency.java
jar cfm TermFrequency.jar MANIFEST.MF *.class
hadoop fs -rm -r /TermFrequency/data/output
hadoop jar TermFrequency.jar TermFrequency
```

## Low Frequency
```
hadoop fs -mkdir -p /LowFrequency/output/
javac -classpath $(hadoop classpath) -d . LowFrequency.java
jar cfm LowFrequency.jar MANIFEST.MF LowFrequency*.class
hadoop fs -rm -r /LowFrequency/data/output
hadoop jar LowFrequency.jar LowFrequency
```

## Top Frequency
```
hadoop fs -mkdir -p /TopFrequency/output/
javac -classpath $(hadoop classpath) -d . Utils.java TopFrequency.java
jar cfm TopFrequency.jar MANIFEST.MF TopFrequency*.class Utils*.class
hadoop fs -rm -r /TopFrequency/data/output
hadoop fs -rm -r /TopFrequency/data/tmp
hadoop jar TopFrequency.jar TopFrequency
```

## TF-IDF
```
hadoop fs -mkdir -p /TfIdf/output/
javac -classpath $(hadoop classpath) -d . TfIdf.java
jar cfm TfIdf.jar MANIFEST.MF TfIdf*.class
hadoop fs -rm -r /TfIdf/data/output
hadoop fs -rm -r /TfIdf/data/tmp
hadoop jar TfIdf.jar TfIdf
```

## Top Average TF-IDF
```
hadoop fs -mkdir -p /TopAvgTfIdf/output/
javac -classpath $(hadoop classpath) -d . Utils.java TopAvgTfIdf.java
jar cfm TopAvgTfIdf.jar MANIFEST.MF *.class
hadoop fs -rm -r /TopAvgTfIdf/data/output
hadoop fs -rm -r /TopAvgTfIdf/data/tmp
hadoop jar TopAvgTfIdf.jar TopAvgTfIdf
```