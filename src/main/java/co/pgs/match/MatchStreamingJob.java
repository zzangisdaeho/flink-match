package co.pgs.match;

import co.pgs.match.api.flatmap.MatchFunction;
import co.pgs.match.api.map.UserInfoEnricher;
import co.pgs.match.deserializer.KafkaMatchRequestDeserializer;
import co.pgs.match.dto.MatchRequest;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;


public class MatchStreamingJob {

//    public static final String HOST_NAME = "localhost";
    public static final String HOST_NAME = "host.docker.internal";

    public static void main(String[] args) throws Exception {
        // Flink 실행 환경 설정
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka 소스 설정
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:29092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "match-group");

        FlinkKafkaConsumer<MatchRequest> kafkaSource = new FlinkKafkaConsumer<>(
                "user-match-requests",
//                new MatchRequestDeserializer(),
                new KafkaMatchRequestDeserializer(),
                properties
        );

        // 데이터 스트림 읽기 및 처리
        DataStream<MatchRequest> stream = env.addSource(kafkaSource);
        stream
                .keyBy(MatchRequest::getUserId)
                .map(new UserInfoEnricher())
                .flatMap(new MatchFunction())
                .addSink(new FlinkKafkaProducer<>(
                        "broker1:29092",
                        "match-results",
                        new SimpleStringSchema()
                ));

        // Flink 작업 실행
        env.execute("Match Streaming Job");
    }
}






