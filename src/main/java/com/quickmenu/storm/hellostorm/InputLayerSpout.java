/**
 * 
 */
package com.quickmenu.storm.hellostorm;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author 	: quickmenu
 * @Date	: 2015. 5. 25.
 * @Type	: hellostorm
 * @Version	: 1.0
 */
public class InputLayerSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "127.0.0.1"); 

	private SpoutOutputCollector collector;
	private Jedis redis = null;
	private String LOGLIST = "logstash";
	
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	// 외부 데이터를 받아 다음 워크 플로우로 전달
	public void nextTuple() {
		redis = jedisPool.getResource();
		try { 
			String content = redis.rpop(LOGLIST);
			if(content != null && !"nil".equalsIgnoreCase(content)) {
				System.out.println("로그체크"+content);
				this.collector.emit(new Values(content));
			} else {
				try {
					Thread.sleep(300);
				} catch (InterruptedException e) {}
			}
		}catch(JedisConnectionException e){ 
			if(null != redis){ 
				jedisPool.returnResourceObject(redis);
				jedisPool.destroy();
				redis = null;
			} 
		}finally{ 
			if(null != redis){ 
				jedisPool.returnResourceObject(redis);
			} 
		}
		//워크플로우로 보내는 함수 emit
		/*
		String content = redis.rpop(LOGLIST);
		if(content == null || "nil".equals(content)) {
			try {
				Thread.sleep(300);
			} catch(InterruptedException e) { }
		} else {
			System.out.println("로그체크"+content);
			this.collector.emit(new Values(content));
		}
		*/
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//필드 값을 정의하는 함수
		declarer.declare(new Fields("say"));
	}
	
}
