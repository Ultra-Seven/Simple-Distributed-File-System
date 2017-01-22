package sdfs.namenode;

import sdfs.Constants;

/**
 * Created by lenovo on 2016/10/1.
 * master process managing name node
 */
public class SDFSMaster {
    private String registerHost = Constants.DEFAULT_IP;
    private int registerPort = Constants.DEFAULT_PORT + 1;
    public SDFSMaster() {
    }

    public static void main(String[] args) {
        NameNodeServer nameNodeServer = new NameNodeServer();
        NameNode nameNode = new NameNode();
        NameNodeMetaData.getNameNode();
        if (!nameNodeServer.isRunning()) {
            nameNodeServer.register(nameNode);
            nameNodeServer.start();
        }
        System.out.println(">>>>>INFO:NameService is binded successfully!");
    }
}
