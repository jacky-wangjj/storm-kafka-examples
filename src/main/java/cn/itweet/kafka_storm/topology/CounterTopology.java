package cn.itweet.kafka_storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import cn.itweet.kafka_storm.bolt.CheckOrderBolt;
import cn.itweet.kafka_storm.bolt.CounterBolt;

public class CounterTopology {

	/**
	 * @param args
	 * http://www.programcreek.com/java-api-examples/index.php?api=storm.kafka.KafkaSpout
	 */
	public static void main(String[] args) {
		try{
			//设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数（6个）
			String zkhost = "10.110.181.39:2181,10.110.181.40:2181,10.110.181.41:2181";
			String topic = "order";
			String groupId = "id";
			int spoutNum = 3;
			int boltNum = 1;
			ZkHosts zkHosts = new ZkHosts(zkhost);//kafaka所在的zookeeper
			SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/order", groupId);  // create /order /id
			spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
			KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", kafkaSpout, spoutNum);
			builder.setBolt("check", new CheckOrderBolt(), boltNum).shuffleGrouping("spout");
	        builder.setBolt("counter", new CounterBolt(),boltNum).shuffleGrouping("check");

	        Config config = new Config();
	        config.setDebug(true);
	        
	        if(args!=null && args.length > 0) {
	            config.setNumWorkers(2);
	            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
	        } else {        
	            config.setMaxTaskParallelism(2);
	
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology("Wordcount-Topology", config, builder.createTopology());

	            Thread.sleep(500000);

	            cluster.shutdown();
	        }
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}