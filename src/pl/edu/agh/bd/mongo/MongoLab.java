package pl.edu.agh.bd.mongo;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;

public class MongoLab {
	private MongoClient mongoClient;
	private static DB db;
	
	public MongoLab() throws UnknownHostException {
		mongoClient = new MongoClient();
		db = mongoClient.getDB("jeopardy");
	}

	public static void main(String[] args) throws UnknownHostException {
		@SuppressWarnings("unused")
		MongoLab mongoLab = new MongoLab();
		DBCollection coll = db.getCollection("question");

		DBObject query = new BasicDBObject("category", "EVERYBODY TALKS ABOUT IT...");
		query.put("round", "Jeopardy!");
		DBObject keys = new BasicDBObject("category", 1);
		keys.put("question", 1);
		keys.put("value", 1);
		keys.put("answer", 1);
		DBCursor cursor = coll.find(query, keys).sort(new BasicDBObject("value", -1));
		
		try {
			while(cursor.hasNext()) {
				System.out.println(cursor.next());
			}
		} finally {
			cursor.close();
		}
		
		System.out.println("\n");
		
		DBObject match = new BasicDBObject("$match", new BasicDBObject("round", "Double Jeopardy!"));
		DBObject fields = new BasicDBObject("category", 1);
		fields.put("total", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		DBObject groupFields = new BasicDBObject("_id", "$category");
		groupFields.put("total", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields);
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("total", -1));
		List<DBObject> pipeline = Arrays.asList(match, project, group, sort);
		AggregationOutput output = coll.aggregate(pipeline);
		
		int howmany = 10;
		for (DBObject result: output.results()) {
			System.out.println(result);
			if (--howmany == 0) break;;
		}
		
		System.out.println("\n");
		
		String map = "function() {" +
					"if(this.round == \"Jeopardy!\") { emit(\"Jeopardy!\", 1) }" +
					"if(this.round == \"Double Jeopardy!\") { emit(\"Double Jeopardy!\", 1) }" +
					"if(this.round == \"Final Jeopardy!\") { emit(\"Final Jeopardy!\", 1) }" +
					"if(this.round == \"Tiebreaker\") { emit(\"Tiebreaker\", 1) } }";
		String reduce = "function(key, values) { return Array.sum(values) }";
		
		MapReduceCommand cmd = new MapReduceCommand(coll, map, reduce, "result", MapReduceCommand.OutputType.INLINE, null);
		MapReduceOutput out = coll.mapReduce(cmd);
		
		for (DBObject o : out.results()) {
			System.out.println(o.toString());
		}
		
	}
}
