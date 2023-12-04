package co.pgs.match.deserializer;

import co.pgs.match.dto.MatchRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class MatchRequestDeserializer implements DeserializationSchema<MatchRequest> {

    private ObjectMapper objectMapper;

    public MatchRequestDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public MatchRequest deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, MatchRequest.class);
    }

    @Override
    public boolean isEndOfStream(MatchRequest nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MatchRequest> getProducedType() {
        return TypeExtractor.getForClass(MatchRequest.class);
    }


}
