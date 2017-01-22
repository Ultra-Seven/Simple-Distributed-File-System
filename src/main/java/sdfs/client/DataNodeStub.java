/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.Server.RPC;
import sdfs.datanode.DataNodeService;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.protocol.IDataNodeProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

public class DataNodeStub implements IDataNodeProtocol {
    IDataNodeProtocol dataNodeProtocol;
    public DataNodeStub(InetSocketAddress dataNodeAddress) {
        //System.out.println(dataNodeAddress.getAddress().getHostAddress() + " " + dataNodeAddress.getPort());
        dataNodeProtocol = RPC.getProxy(IDataNodeProtocol.class, dataNodeAddress.getAddress().getHostAddress(), dataNodeAddress.getPort());
    }

    @Override
    public void delete(int blockNumber) throws IOException {
        dataNodeProtocol.delete(blockNumber);
        Exception exception = dataNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof FileNotFoundException)
                throw (FileNotFoundException)exception;
            else
                throw (IOException) exception;
        }
    }

    @Override
    public Exception getException() {
        return dataNodeProtocol.getException();
    }

    @Override
    public byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        byte[] bytes = dataNodeProtocol.read(fileAccessToken, blockNumber, position, size);
        Exception exception = dataNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IllegalStateException)
                throw (IllegalStateException)exception;
            else if (exception instanceof IndexOutOfBoundsException)
                throw (IndexOutOfBoundsException)exception;
            else if (exception instanceof IllegalAccessTokenException)
                throw (IllegalAccessTokenException)exception;
            else if (exception instanceof IllegalArgumentException)
                throw (IllegalArgumentException)exception;
            else
                throw (FileNotFoundException)exception;
        }
        return bytes;
    }

    @Override
    public void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        dataNodeProtocol.write(fileAccessToken, blockNumber, position, buffer);
        Exception exception = dataNodeProtocol.getException();
        if (exception != null) {
            if (exception instanceof IndexOutOfBoundsException)
                throw (IndexOutOfBoundsException)exception;
            else
                throw (IllegalStateException)exception;
        }
    }
}
