package hadoop.demo.wordcountdemo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;

public class WordCount {

	public static MongoClient mongo = new MongoClient("localhost", 27017);
	public static DB db = mongo.getDB("Hadoop");
	public static DBCollection collectionMap = db.getCollection("WordCountMap");
	public static DBCollection collectionReduce = db
			.getCollection("WordCountReduce");

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println(key.toString() + "--" + value);
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
				
//				System.out.println(word + " --- " + one);
//
//				DBObject document = new BasicDBObject();
//				document.put("value", word + " --- " + one);
//				collectionMap.insert(document);
			}
//			DBCursor cursor = db.getCollection("WordCountMap").find();
//			while (cursor.hasNext()) {
//		        System.out.println(cursor.next());
//		    }
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);

//			System.out.println("Result " + key + " --- " + result);
//
//			DBObject document = new BasicDBObject();
//			document.put("Result", key + " --- " + result);
//			collectionReduce.insert(document);
//			
//			DBCursor cursor = db.getCollection("WordCountReduce").find();
//			while (cursor.hasNext()) {
//		        System.out.println(cursor.next());
//		    }

		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Begin -------------------------");
		long begin = System.currentTimeMillis();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println("End -------------------------");
		System.out.println(end-begin);
	}
}