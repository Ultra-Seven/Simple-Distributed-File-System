package sdfs.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by lenovo on 2016/10/22.
 */
public class Listener extends Thread {
    private ServerSocket socket;
    private Server server;
    public Listener(Server server) {
        this.server = server;
    }

    @Override
    public void run() {
        System.out.println("The server is running at:" + server.getPort());
        try {
            socket = new ServerSocket(server.getPort());
            while (server.isRunning()) {
                Socket clientSocket = socket.accept();
                ServiceThread thread = new ServiceThread(clientSocket, server);
                thread.run();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (socket != null && !socket.isClosed())
                socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static final class ServiceThread implements Runnable {
        private Socket socket;
        private Server server;

        public ServiceThread(Socket socket, Server server) {
            this.socket = socket;
            this.server = server;
        }
        @Override
        public void run() {
            try {
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Invocation invocation = (Invocation) ois.readObject();
                server.call(invocation);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(invocation);
                oos.flush();
                oos.close();
                ois.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
