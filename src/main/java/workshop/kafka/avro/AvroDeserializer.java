package workshop.kafka.avro;

import org.apache.kafka.common.serialization.Deserializer;
import workshop.kafka.model.User;

public class AvroDeserializer implements Deserializer<User> {

    @Override
    public User deserialize(String topic, byte[] data) {
        return AvroUserHelper.fromUserRecordWithoutSchema(data);
    }
}
