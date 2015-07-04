/**
 * 
 */
package com.quickmenu.storm.proto;

import static com.mongodb.client.model.Filters.eq;

import org.bson.Document;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * @author 	: quickmenu
 * @Date	: 2015. 6. 27.
 * @Type	: PrototypeStorm
 * @Version	: 1.0
 */
public class OutLayerBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	private static final MongoClient mongo = new MongoClient("localhost", 27017);
	private int count = 1;
	
	//HellowSpout 생성데이터 들어오면 실행(자동)
	//데이터 처리가 끝나면 collector.emit을 이용해 다음 플로우로 전달
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		MongoDatabase mongoDB = mongo.getDatabase("testdb");
		MongoCollection<Document> collection = mongoDB.getCollection("testlog");
		Document find = collection.find(eq("type", "access")).first();
		
		String value = tuple.getStringByField("access");
		
		if("bigdata/main.do".equalsIgnoreCase(value)) {
			if(find == null) {
				Document insert = new Document("type", "access").append("count", count);
				collection.insertOne(insert);
			} else {
				count = find.getInteger("count");
				count = count+1;
				// update
				collection.updateOne(eq("type", "access"), new Document("$set", new Document("count", count)));
			}
		}
	}

	//declarer에서 필드 정의
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
