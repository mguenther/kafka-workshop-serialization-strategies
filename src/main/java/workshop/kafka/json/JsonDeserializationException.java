package workshop.kafka.json;

public class JsonDeserializationException extends RuntimeException {

    private final byte[] faultyMessageString;

    public JsonDeserializationException(byte[] faultyMessage, String message, Throwable ex) {
        this.faultyMessageString = faultyMessage;
    }

    public byte[] getFaultyMessageString() {
        return faultyMessageString;
    }
}
