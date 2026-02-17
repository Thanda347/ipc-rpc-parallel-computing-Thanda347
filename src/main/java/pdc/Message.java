package pdc;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;


/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {

    public static final String Register = "Register";
    public static final String Task = "Task";
    public static final String Response = "Response";
    public static final String Heartbeat = "Heartbeat";
    public static final String Shutdown = "Shutdown";
    public static final String ack = "ack";


    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;
    public int taskId;
    public int workerId;


    public Message() {
        this.magic = "ABCD";
        this.version = 1;
        this.type = "";
        this.sender = "";
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
        this.taskId = 0;
        this.workerId = 0;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        ByteArrayOutputStream byteArrayOutStr = new ByteArrayOutputStream();
        DataOutputStream dataOutStr = new DataOutputStream(byteArrayOutStr);
    
        try{
            byte[] magicdata = magic.getBytes(StandardCharsets.UTF_8);
            dataOutStr.writeInt(magicdata.length);
            dataOutStr.write(magicdata);
            dataOutStr.writeInt(version);
            byte[] typedata = type.getBytes(StandardCharsets.UTF_8);
            dataOutStr.writeInt(typedata.length);
            dataOutStr.write(typedata);
            byte[] senderdata =sender.getBytes(StandardCharsets.UTF_8);
            dataOutStr.writeInt(senderdata.length);
            dataOutStr.write(senderdata);
            dataOutStr.writeLong(timestamp);
            dataOutStr.writeInt(payload.length);
            dataOutStr.write(payload);
            dataOutStr.writeInt(taskId);
            dataOutStr.writeInt(workerId);
             
            dataOutStr.flush();
            return byteArrayOutStr.toByteArray();
  } catch (IOException e){
    System.err.println("Error packing message");
             return new byte[0];
  }    
    
}

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(DataInputStream dataInpStr) throws IOException {
    

        try{

            int magiclength = dataInpStr.readInt();
            byte[] magicdata = new byte[magiclength];
            dataInpStr.readFully(magicdata);
            int version = dataInpStr.readInt();
            int typelength = dataInpStr.readInt();
            byte[] typedata = new byte[typelength];
            dataInpStr.readFully(typedata);
            int senderlength = dataInpStr.readInt();
            byte[] senderdata = new byte[senderlength];
            dataInpStr.readFully(senderdata);
            long timestamp = dataInpStr.readLong();
            int payloadlength = dataInpStr.readInt();
            byte[] payload = new byte[payloadlength];
            dataInpStr.readFully(payload);
            int taskId = dataInpStr.readInt();
            int workerId = dataInpStr.readInt();

            Message msg = new Message();
            msg.magic = new String(magicdata, StandardCharsets.UTF_8);

            if (!msg.magic.equals("ABCD")) {
                System.err.println("Invalid magic string: " + msg.magic);
                return null;
            }
            msg.version = version;
            msg.type = new String(typedata, StandardCharsets.UTF_8);
            msg.sender = new String(senderdata, StandardCharsets.UTF_8);
            msg.timestamp = timestamp;
            msg.payload = payload;
            msg.taskId = taskId;
            msg.workerId = workerId;

            return msg;
        } catch (IOException e) {
             System.err.println("Error unpacking message");
             return null;
        }

    }
}
