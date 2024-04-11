package workshop.kafka.avro;

import org.apache.kafka.common.serialization.Serializer;
import workshop.kafka.model.User;

public class AvroSerializer implements Serializer<User> {

    @Override
    public byte[] serialize(String topic, User data) {
        return AvroUserHelper.toUserRecordWithoutSchema(data);
    }
}
