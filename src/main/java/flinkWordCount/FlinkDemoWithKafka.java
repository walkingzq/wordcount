package flinkWordCount;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * Create by Zhao Qing on 2018/4/20
 * flink消费kafka数据并写入kafka
 */
public class FlinkDemoWithKafka {
    public static void main(String[] args) throws Exception{
        String kafka_server = "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092";
        String kafka_zk = "10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1";
        String groupId = "flink_test";
        String topic = "test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka_server);
        properties.setProperty("zookeeper.connect", kafka_zk);
        properties.setProperty("group.id", groupId);
        properties.setProperty("print.timestamp", "true");//显示时间戳
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();//获取flink运行环境
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//设置时间格式为eventtime
        env.enableCheckpointing(5000);//flink checkpoint 间隔，5000ms
        //创建一个kafka消费者，注意flink支持topic的正则表达式（即可以根据指定规则自动发现kafka topic并进行消费，默认offset为最早）
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromEarliest();//设定consumer的offset为最早


//        kafkaConsumer010.assignTimestampsAndWatermarks(new  );//TODO:eventtime 待测验
        //创建一个flink DataStream
        DataStream<String> stream = env.addSource(kafkaConsumer010);


        //创建一个kafka producer
        String broker = "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092";
        String producerTopic = "producer_test";
        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(broker, producerTopic, new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);//设定是否将timestamp写入kafka

        FlinkKafkaProducer010<String> producer010 = new FlinkKafkaProducer010<String>(broker, "topic2", new SimpleStringSchema());
        producer010.setWriteTimestampToKafka(false);


        stream.addSink(myProducer);//添加sink
        stream.addSink(producer010);
//        stream.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq"));//添加sink
        env.execute("kafka-test");//开始执行
    }
}
