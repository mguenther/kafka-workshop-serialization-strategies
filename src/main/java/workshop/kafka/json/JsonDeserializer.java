package workshop.kafka.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import workshop.kafka.model.User;

import java.io.IOException;

public class JsonDeserializer implements Deserializer<User> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, User.class);
        } catch (IOException e) {
            throw new JsonDeserializationException(data, e.getMessage(), e);
        }
    }
}
