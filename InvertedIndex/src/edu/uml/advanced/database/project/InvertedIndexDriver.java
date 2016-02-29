package edu.uml.advanced.database.project;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.DBCollection;

// this class sets up an environment for map reduce job.
// set input path
// set output path
// set mapper class
// set reducer class
// read list of insignificant words

public class InvertedIndexDriver {

	static MongoClient clientMongo;
	static DB db;
	static DBCollection collectionMongo;
	static long startTime;
	static Scanner scanner;

	// to store list of insignificant words for which we do not wish to build
	// inverted index
	static ArrayList inSignificantWords = new ArrayList<String>();

	public static void main(String[] args) {

		JobClient client = new JobClient();
		JobConf conf = new JobConf(InvertedIndexDriver.class);

		// specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// specify input and output dirs
		FileInputFormat.addInputPath(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));

		// specify a mapper
		conf.setMapperClass(InvertedIndexMapper.class);

		// specify a reducer
		conf.setReducerClass(InvertedIndexReducer.class);
		conf.setCombinerClass(InvertedIndexReducer.class);

		client.setConf(conf);
		try {
			Scanner scanner = new Scanner(new File(
					"input/insignificant_words.txt"));

			while (scanner.hasNext()) {
				inSignificantWords.add(scanner.next());
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			clientMongo = new MongoClient();
			db = clientMongo.getDB("inv_index");
			collectionMongo = db.getCollection("indexes");

			startTime = System.currentTimeMillis();
			JobClient.runJob(conf);
		} catch (IOException e) {

			e.printStackTrace();
		}
		System.out.println("####################");
		Double timeToExecute = (double) ((System.currentTimeMillis() - startTime) / 1000d);
		System.out.println("Total Time of execution: " + timeToExecute
				+ " seconds");
		System.out.println("####################");

	}
}