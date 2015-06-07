/**
 * 
 */
package com.quickmenu.storm.hellostorm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author 	: quickmenu
 * @Date	: 2015. 5. 25.
 * @Type	: hellostorm
 * @Version	: 1.0
 */
public class HelloBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;

	//HellowSpout 생성데이터 들어오면 실행(자동)
	//데이터 처리가 끝나면 collector.emit을 이용해 다음 플로우로 전달
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String value = tuple.getStringByField("say");
		System.out.println("Tuple value is " + value);
	}

	//declarer에서 필드 정의
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
