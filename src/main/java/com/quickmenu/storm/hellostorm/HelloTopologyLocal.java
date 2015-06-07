/**
 * 
 */
package com.quickmenu.storm.hellostorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * @author 	: quickmenu
 * @Date	: 2015. 5. 25.
 * @Type	: hellostorm
 * @Version	: 1.0
 */
public class HelloTopologyLocal {
	public static void main(String args[]) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("HelloSpout", new HelloSpout(), 2);
		// suffleGrouping 메스드를 이용하여 HelloBolt가 HelloSpout로부터 생성되는 데이터를 읽어드림을 명시
		builder.setBolt("HelloBolt", new HelloBolt(),4).shuffleGrouping("HelloSpout");
		
		// 토플로지가 어떤 서버에서 어떤 포트등을 이용해서 실행되는지 정의
		Config conf = new Config();
		conf.setDebug(true);
		// 개발환경용 클러스터
		LocalCluster cluster = new LocalCluster();
		
		// 토폴로지를 배포[토폴로지 자동 실행]
		cluster.submitTopology("HelloTopologyLocal", conf, builder.createTopology());
		// 토폴로지는 자동으로 쓰레드로 진행(10초동안)
		Utils.sleep(10000);
		
		// kill the LearningStromTopology
		cluster.killTopology("HelloTopologyLocal");
		// shutdown the storm test cluster
		cluster.shutdown();
	}
}
