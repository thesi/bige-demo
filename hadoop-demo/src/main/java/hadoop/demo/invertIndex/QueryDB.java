package hadoop.demo.invertIndex;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class QueryDB {
	static MongoClient clientMongo;

	public static void main(String arg[]) {

		findDocsContaining(arg[0]);

	}

	private static void findDocsContaining(String keyword) {
		// TODO Auto-generated method stub
		try {
			clientMongo = new MongoClient();
			DB db = clientMongo.getDB("inv_index");
			DBCollection collectionMongo = db.getCollection("indexes");
			DBCursor cursor = collectionMongo.find(new BasicDBObject("word",
					keyword));
			if (cursor.count() == 0)
				System.out.println("No keyword found");
			while (cursor.hasNext()) {
				System.out.println(cursor.next().get("File"));
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Could not connect to Database");
			e.printStackTrace();
		}

	}

}
