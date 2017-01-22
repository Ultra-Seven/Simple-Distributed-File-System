package sdfs.datanode;

import sdfs.Constants;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by lenovo on 2016/10/1.
 * Slave process
 * responsible for managing blocks and send heart beat
 */
public class SDFSSlave implements Serializable {
    private static final long serialVersionUID = 1L;
    private static int registryPort = Constants.DEFAULT_PORT;
    private static String registryHost = Constants.DEFAULT_IP;
    private static DataNodeService slaveService = new DataNodeService();
    //private static MyThread heartBeat;
    public static int maxBlockId;
    public static DataNode dataNode = new DataNode();
    public SDFSSlave()  {
        maxBlockId = slaveService.getMaxBlockId();
    }
    public static void main(String[] args) {
        //heart beat thread
        class MyThread extends TimerTask {
            public void run() {
                heart_beat();
            }
        }
        class GarbageCollector extends TimerTask {
            private Set<Integer> garbage = dataNode.getGarbage();
            @Override
            public void run() {
                System.out.println("garbage is working!");
                garbage.forEach(integer -> {
                    slaveService.removeFile(integer);
                    garbage.remove(integer);
                });
            }
        }
        Timer timer = new Timer();
        Timer timer2 = new Timer();
        GarbageCollector garbageCollector = new GarbageCollector();
        DataNodeServer dataNodeServer = new DataNodeServer();
        MyThread myThread = new MyThread();
        if (!dataNodeServer.isRunning()) {
            dataNodeServer.register(dataNode);
            dataNodeServer.start();

        }
        System.out.println(">>>>>INFO:DataNode is binded successfully");
        timer.schedule(garbageCollector, 0, 2000000);
        timer2.schedule(myThread, 0, 60000);
    }
    //heart beat
    private static void heart_beat() {
        System.out.println(">>>>>>>>>INFO:heart beat from data node");
        new NameNodeDataNodeStub(new InetSocketAddress(Constants.DEFAULT_IP, Constants.DEFAULT_PORT)).sendHeartBeat(registryHost, registryPort);
    }
}

