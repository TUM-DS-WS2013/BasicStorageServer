package common.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author Danila Klimenko
 */
public class NetworkMessage {
    private static final int    MAX_MESSAGE_SIZE = 128 * 1024;
    private static final int    SIZEOF_LENGTH = 4;
    
    private final int       length;
    private final byte[]    data;
    
    public NetworkMessage(byte[] data) throws IOException {
        if (data.length > MAX_MESSAGE_SIZE) {
            throw new IOException("Error! Message size limit exceeded.");
        }
        
        this.data = data;
        this.length = data.length;
    }
    
    public byte[] getData() {
        return this.data;
    }
    
    public void writeTo(OutputStream os) throws IOException {
        ByteBuffer bbuf = ByteBuffer.allocate(SIZEOF_LENGTH + this.length);
        
        bbuf.putInt(this.length);
        bbuf.put(this.data);
        
        os.write(bbuf.array());
    }
    
    public static NetworkMessage readFrom(InputStream is) throws IOException {
        DataInputStream dis = new DataInputStream(is);
        
        int length = dis.readInt();
        if (length > MAX_MESSAGE_SIZE) {
            throw new IOException("Error! Message size limit exceeded.");
        }
        
        byte[] data = new byte[length];
        
        int read_bytes = dis.read(data);
        if (read_bytes != length) {
            throw new IOException("Error! Message is incomplete: expected length = " +
                                    length + "; available = " + read_bytes + ".");
        }
        
        return new NetworkMessage(data);
    }
}
