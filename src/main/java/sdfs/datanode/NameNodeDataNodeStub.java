package sdfs.datanode;

import sdfs.Server.RPC;
import sdfs.namenode.AccessTokenPermission;
import sdfs.namenode.LocatedBlock;
import sdfs.protocol.INameNodeDataNodeProtocol;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by lenovo on 2016/11/21.
 */
public class NameNodeDataNodeStub implements INameNodeDataNodeProtocol {
    INameNodeDataNodeProtocol iNameNodeDataNodeProtocol;
    public NameNodeDataNodeStub(InetSocketAddress nameNodeAddress) {
        iNameNodeDataNodeProtocol = RPC.getProxy(INameNodeDataNodeProtocol.class, nameNodeAddress.getAddress().getHostAddress(), nameNodeAddress.getPort());
    }
    @Override
    public AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken, InetAddress inetAddress) throws RemoteException {
        AccessTokenPermission accessTokenPermission = iNameNodeDataNodeProtocol.getAccessTokenOriginalPermission(fileAccessToken, inetAddress);
        Exception exception = iNameNodeDataNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof RemoteException)
                throw (RemoteException)exception;
        }
        return accessTokenPermission;
    }

    @Override
    public Map<Integer, Integer> getAccessTokenNewBlocks(UUID fileAccessToken, InetAddress inetAddress) throws RemoteException {
        Map<Integer, Integer> maps = iNameNodeDataNodeProtocol.getAccessTokenNewBlocks(fileAccessToken, inetAddress);
        Exception exception = iNameNodeDataNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof RemoteException)
                throw (RemoteException)exception;
        }
        return maps;
    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException, RemoteException {
        LocatedBlock block = iNameNodeDataNodeProtocol.newCopyOnWriteBlock(fileAccessToken, fileBlockNumber);
        Exception exception = iNameNodeDataNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof RemoteException)
                throw (RemoteException)exception;
        }
        return block;
    }

    @Override
    public void sendHeartBeat(String ip, int port) {
        iNameNodeDataNodeProtocol.sendHeartBeat(ip, port);
    }

    @Override
    public Exception getException() {
        return iNameNodeDataNodeProtocol.getException();
    }
}
