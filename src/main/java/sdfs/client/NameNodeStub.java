/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.namenode.LocatedBlock;
import sdfs.Server.RPC;
import sdfs.namenode.SDFSFileChannelData;
import sdfs.protocol.INameNodeProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {
    private INameNodeProtocol nameNodeProtocol;
    public NameNodeStub(InetSocketAddress nameNodeAddress, int fileDataBlockCacheSize) {
        System.out.println(nameNodeAddress.getAddress().getHostAddress() + " " + nameNodeAddress.getPort());
        nameNodeProtocol = RPC.getProxy(INameNodeProtocol.class, nameNodeAddress.getAddress().getHostAddress(), nameNodeAddress.getPort());
        setFileDataBlockCacheSize(fileDataBlockCacheSize);
    }
    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannel = nameNodeProtocol.openReadonly(fileUri);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof FileNotFoundException)
                throw (FileNotFoundException)exception;
        }
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws IOException, IllegalStateException {
        SDFSFileChannelData sdfsFileChannel = nameNodeProtocol.openReadwrite(fileUri);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof FileNotFoundException)
                throw (FileNotFoundException)exception;
            else
                throw  (IllegalStateException)exception;
        }
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannel = nameNodeProtocol.create(fileUri);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof SDFSFileAlreadyExistException)
                throw (SDFSFileAlreadyExistException)exception;
            else
                throw (FileNotFoundException)exception;
        }
        return sdfsFileChannel;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalAccessTokenException, IOException {
        nameNodeProtocol.closeReadonlyFile(fileUuid);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IllegalAccessTokenException)
                throw (IllegalAccessTokenException)exception;
        }
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, long newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        nameNodeProtocol.closeReadwriteFile(fileUuid, newFileSize);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IllegalAccessTokenException)
                throw (IllegalAccessTokenException)exception;
            else
                throw (IllegalArgumentException)exception;
        }
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        nameNodeProtocol.mkdir(fileUri);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof SDFSFileAlreadyExistException)
                throw (SDFSFileAlreadyExistException)exception;
            else if (exception instanceof FileNotFoundException)
                throw (FileNotFoundException)exception;
        }
    }
/*
    @Override
    public LocatedBlock getBlock(UUID fileUuid, int blockNumber) throws IndexOutOfBoundsException {
        LocatedBlock locatedBlock = nameNodeProtocol.getBlock(fileUuid, blockNumber);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IndexOutOfBoundsException)
                throw (IndexOutOfBoundsException)exception;
        }
        return locatedBlock;
    }

    @Override
    public Map<Integer, LocatedBlock> getBlocks(UUID fileUuid, int startBlockNumber, int blockAmount) throws IllegalStateException, IndexOutOfBoundsException {
        Map<Integer, LocatedBlock> map = nameNodeProtocol.getBlocks(fileUuid, startBlockNumber, blockAmount);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IndexOutOfBoundsException)
                throw (IndexOutOfBoundsException)exception;
            else
                throw (IllegalStateException)exception;
        }
        return map;
    }

    @Override
    public Map<Integer, LocatedBlock> getBlocks(UUID fileUuid, int[] blockNumbers) throws IllegalStateException, IndexOutOfBoundsException {
        Map<Integer, LocatedBlock> map = nameNodeProtocol.getBlocks(fileUuid, blockNumbers);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IndexOutOfBoundsException)
                throw (IndexOutOfBoundsException)exception;
            else
                throw (IllegalStateException)exception;
        }
        return map;
    }
*/
    /*
    public LocatedBlock addBlock(UUID fileUuid) throws IllegalStateException {
        LocatedBlock locatedBlock = nameNodeProtocol.addBlock(fileUuid);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IllegalStateException)
                throw (IllegalStateException)exception;
        }
        return locatedBlock;
    }*/

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException, RemoteException {
        List<LocatedBlock> locatedBlocks = nameNodeProtocol.addBlocks(fileUuid, blockAmount);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IllegalStateException)
                throw (IllegalStateException)exception;
        }
        return locatedBlocks;
    }

//    @Override
//    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
//        nameNodeProtocol.removeLastBlock(fileUuid);
//        Exception exception = nameNodeProtocol.getException();
//        if (exception != null) {
//            if (exception instanceof IllegalStateException)
//                throw (IllegalStateException)exception;
//        }
//    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException, RemoteException {
        nameNodeProtocol.removeLastBlocks(fileUuid, blockAmount);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IllegalStateException)
                throw (IllegalStateException)exception;
        }
    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException, RemoteException {
        LocatedBlock block = nameNodeProtocol.newCopyOnWriteBlock(fileAccessToken, fileBlockNumber);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IllegalStateException)
                throw (IllegalStateException)exception;
        }
        return block;
    }

    @Override
    public void rm(String fileUri) throws FileNotFoundException {
        nameNodeProtocol.rm(fileUri);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof FileNotFoundException)
                throw (FileNotFoundException)exception;
        }
    }

    @Override
    public void rename(String fileUri, String newName) throws FileNotFoundException {
        nameNodeProtocol.rename(fileUri, newName);
        Exception exception = nameNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof FileNotFoundException)
                throw (FileNotFoundException)exception;
        }
    }

    @Override
    public Exception getException() {
        return nameNodeProtocol.getException();
    }

    @Override
    public void setFileDataBlockCacheSize(int size) {
        nameNodeProtocol.setFileDataBlockCacheSize(size);
    }

    @Override
    public void setBlocks(UUID fileUuid, ArrayList<LocatedBlock> blocks) throws ClosedChannelException {
        nameNodeProtocol.setBlocks(fileUuid, blocks);
    }
}
