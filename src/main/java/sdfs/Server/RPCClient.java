package sdfs.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by lenovo on 2016/10/25.
 */
public class RPCClient {
    private String ip;
    private int port;
    private Socket socket;
    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;
    public RPCClient(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }
    public void call(Invocation invocation) throws ClassNotFoundException, IOException {
        socket = new Socket(ip, port);
        objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        objectOutputStream.writeObject(invocation);
        objectOutputStream.flush();
        objectInputStream = new ObjectInputStream(socket.getInputStream());
        Invocation result = (Invocation) objectInputStream.readObject();
        invocation.setResult(result.getResult());
    }
}
