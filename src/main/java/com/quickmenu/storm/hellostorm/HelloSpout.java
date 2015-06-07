/**
 * 
 */
package com.quickmenu.storm.hellostorm;

import java.util.Map;

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
public class HelloSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	// 외부 데이터를 받아 다음 워크 플로우로 전달
	public void nextTuple() {
		//워크플로우로 보내는 함수 emit
		this.collector.emit(new Values("hellow world"));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//필드 값을 정의하는 함수
		declarer.declare(new Fields("say"));;
	}
	
}
