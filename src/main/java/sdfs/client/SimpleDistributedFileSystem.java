/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.Constants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.FileAlreadyExistsException;

public class SimpleDistributedFileSystem implements ISimpleDistributedFileSystem {
    NameNodeStub nameNodeStub;
    DFSClient dfsClient = new DFSClient(Constants.DEFAULT_IP, Constants.DEFAULT_PORT);
    public SimpleDistributedFileSystem(InetSocketAddress nameNodeAddress, int fileDataBlockCacheSize) {
        //todo your code here
        nameNodeStub = new NameNodeStub(nameNodeAddress, fileDataBlockCacheSize);
        dfsClient.setNameNodeStub(nameNodeStub);
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        //todo your code here
        SDFSFileChannel sdfsFileChannel = dfsClient.openReadonly(fileUri);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        //todo your code here
        SDFSFileChannel sdfsFileChannel = dfsClient.create(fileUri);
        if (sdfsFileChannel.getException() != null) {
            FileAlreadyExistsException exception =  new FileAlreadyExistsException(sdfsFileChannel.getException().getMessage());
            throw exception;
        }
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        //todo your code here
        SDFSFileChannel sdfsFileChannel = dfsClient.openReadWrite(fileUri);
        if (sdfsFileChannel.getException() != null) {
            FileNotFoundException exception =  new FileNotFoundException(sdfsFileChannel.getException().getMessage());
            throw exception;
        }
        return sdfsFileChannel;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        //todo your code here
        dfsClient.mkdir(fileUri);

    }

    @Override
    public void delete(String fileUri) throws FileNotFoundException{
        dfsClient.rm(fileUri);
    }

    @Override
    public void rename(String fileUri, String newName) throws FileNotFoundException {
        dfsClient.rename(fileUri, newName);
    }
}
