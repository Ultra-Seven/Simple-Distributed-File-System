package sdfs.protocol;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.namenode.AccessTokenPermission;
import sdfs.namenode.LocatedBlock;

import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface INameNodeDataNodeProtocol extends Remote {
    /**
     * Get original file access token permission
     *
     * @return Access token permission
     */
    AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken, InetAddress inetAddress) throws RemoteException;

    /**
     * Get new allocated block of this file
     *
     * @return New allocated block of this file
     */
    Map<Integer, Integer> getAccessTokenNewBlocks(UUID fileAccessToken, InetAddress inetAddress) throws RemoteException;


    /**
     * Request a new copy on write block in order to prevent destroy the original block
     *
     * @param fileBlockNumber the block number in the file that require copy on write
     * @return a locatedBlock than could be used as copy on write block
     * @throws IllegalStateException if there is already copy on write on this file block
     */
    LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException, RemoteException;

    void sendHeartBeat(String ip, int port);

    Exception getException();
}
