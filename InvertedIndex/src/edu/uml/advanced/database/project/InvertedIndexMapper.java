package edu.uml.advanced.database.project;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndexMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	// private final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Text fileName = new Text();

	@Override
	public void map(LongWritable key, Text value, OutputCollector output,
			Reporter reporter) throws IOException {

		String line = value.toString();
		FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
		String location = fileSplit.getPath().getName();
		fileName.set(location);

		StringTokenizer itr = new StringTokenizer(line.toLowerCase());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			output.collect(word, fileName); // emits (keyword,filename) pair
		}
	}
}
