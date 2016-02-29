package edu.uml.advanced.database.project;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class InvertedIndexReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator values, OutputCollector output,
			Reporter reporter) throws IOException {

		// add filenames to inverted index only if word is not insignificant
		if (!InvertedIndexDriver.inSignificantWords.contains(key.toString())) {
			boolean isFirstFile = true;
			StringBuilder toReturn = new StringBuilder();
			while (values.hasNext()) {
				if (!isFirstFile)
					toReturn.append(", ");
				isFirstFile = false;
				toReturn.append(values.next().toString()); // append to existing
															// filesnames
			}

			output.collect(key, new Text(toReturn.toString()));

			// write result to mongoDB
			DBCursor cursor = InvertedIndexDriver.collectionMongo
					.find(new BasicDBObject("word", key.toString()));

			try {
				if (cursor.length() > 0) {
					BasicDBObject document = new BasicDBObject();
					document.put("word", key.toString());
					InvertedIndexDriver.collectionMongo.remove(document);

				}
			} catch (Exception e) {
				System.out.println("");
			}

			String json1 = "{'word':'" + key + "','File':'"
					+ toReturn.toString() + "'}}";
			DBObject obj1 = (DBObject) JSON.parse(json1);
			InvertedIndexDriver.collectionMongo.insert(obj1);
		}
	}

}
