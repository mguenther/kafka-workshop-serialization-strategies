package workshop.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import workshop.kafka.avro.model.UserRecord;
import workshop.kafka.model.User;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class AvroUserHelper {

    private static final Schema userSchema;

    static {
        try {
            userSchema = new Schema.Parser().parse(AvroProducer.class.getClassLoader().getResourceAsStream("avro/user.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toUserRecordWithoutSchema(User user) {
        try {
            var userRecord = new UserRecord(user.getName(), user.getAge());
            return UserRecord.getEncoder().encode(userRecord).array();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static User fromUserRecordWithoutSchema(byte[] data) {
        try {
            var userRecord = UserRecord.getDecoder().decode(data);
            return new User(userRecord.getName().toString(), userRecord.getAge());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    public static User fromGenericRecordWithReadSchema(byte[] data) {
        var reader = new GenericDatumReader<GenericRecord>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(data), reader)) {
            if (dataFileReader.hasNext()) {
                GenericRecord result = dataFileReader.next();
                return new User((String) result.get("name"), (int) result.get("age"));
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toGenericRecordWithReadSchema(User user) {
        var datumWriter = new GenericDatumWriter<GenericRecord>(userSchema);
        var record = new GenericData.Record(userSchema);
        record.put("name", user.getName());
        record.put("age", user.getAge());

        var out = new ByteArrayOutputStream();
        var dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            dataFileWriter.create(userSchema, out);
            dataFileWriter.append(record);
            dataFileWriter.close();

            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
