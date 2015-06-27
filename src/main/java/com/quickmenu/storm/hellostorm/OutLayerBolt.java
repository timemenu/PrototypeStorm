/**
 * 
 */
package com.quickmenu.storm.hellostorm;

import java.util.NoSuchElementException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.quickmenu.storm.util.MongoManager;

/**
 * 
 * @author 	: quickmenu
 * @Date	: 2015. 6. 27.
 * @Type	: PrototypeStorm
 * @Version	: 1.0
 */
public class OutLayerBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	private MongoClient mongo = null;

	//HellowSpout 생성데이터 들어오면 실행(자동)
	//데이터 처리가 끝나면 collector.emit을 이용해 다음 플로우로 전달
	@SuppressWarnings("deprecation")
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			mongo = MongoManager.getMongo(); // pool 객체 가져오기
			DB db = mongo.getDB("testdb");
			DBCollection table = db.getCollection("testlog");
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put("type", "access");
			
			DBCursor cursor = table.find(searchQuery);
			
			String value = tuple.getStringByField("say");
			if(value != null) {
				if(cursor.hasNext()) {
					int count = (Integer)cursor.next().get("count");
					// update
					BasicDBObject query = new BasicDBObject();
					query.put("type", "access");
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.put("count", count+1);
					BasicDBObject updateObj = new BasicDBObject();
					updateObj.put("$set", newDocument);
					table.update(query, updateObj);
				} else {
					// insert
					int count = value.indexOf("quickmenu");
					BasicDBObject document = new BasicDBObject();
					document.put("type", "access");
					if(count > 0) {
						document.put("count", 1);
					}
					table.insert(document);
				}
			}
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if(mongo != null)
					MongoManager.closeMongo(mongo); // pool 객체 반환
			} catch (Exception e) {
				
			}
		}
	}

	//declarer에서 필드 정의
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
