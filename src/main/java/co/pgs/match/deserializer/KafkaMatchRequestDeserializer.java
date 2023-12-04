package co.pgs.match.deserializer;

import co.pgs.match.dto.MatchRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class KafkaMatchRequestDeserializer implements KafkaDeserializationSchema<MatchRequest> {

    private ObjectMapper objectMapper;

    public KafkaMatchRequestDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public boolean isEndOfStream(MatchRequest nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MatchRequest> getProducedType() {
        return TypeInformation.of(MatchRequest.class);
    }

    @Override
    public MatchRequest deserialize(ConsumerRecord<byte[], byte[]> record) throws IOException {
        return objectMapper.readValue(record.value(), MatchRequest.class);
    }
}