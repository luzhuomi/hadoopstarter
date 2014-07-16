package org.collamine.hadoopstarter;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {
    public static class MMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
	        context.write(word, one);
	    }
	}
    }
    
    public static class MReduce extends Reducer<Text,IntWritable, Text,IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { // the change from Iterator to Iterable is quite annoying. The base class Reducer is abstract, having a default reduce() method. when we mixup Itrator with Iterable, there is no compilation error, and the user defined reduce() is never executed. Very bad experience.
	    int sum = 0;
	    for (IntWritable val:values) {
		sum += val.get();
	    }
	    context.write(key, new IntWritable(sum));	    
	}
    }

    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf, "wordcount");
	job.setJarByClass(WordCount.class);
	
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setMapperClass(MMap.class);
	job.setReducerClass(MReduce.class);
	job.setCombinerClass(MReduce.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	System.exit(job.waitForCompletion(true)?0:1);
	return 0;
    }
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new WordCount(), args);
	System.exit(res);
    }
}


/*
== compilation ==

javac -classpath  /opt/hadoop-1.2.1/hadoop-core-1.2.1.jar WordCount.java -d bin 
jar -cvf wordcountnapi.jar -C bin/ .

== execution ==
hadoop dfs -rmr output
/opt/hadoop-1.2.1/bin/hadoop jar /Users/luzm/code/hadoop/WordCount/wordcountnapi.jar org.collamine.hadoopstarter.WordCount input/ output/

 */