package sdfs.protocol;

import sdfs.exception.IllegalAccessTokenException;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;

public interface IDataNodeProtocol extends Remote {
    /**
     * Read data from a block.
     * It should be redirect to [dataBlockNumber].block file
     *
     * @param fileAccessToken the file accessToken to check whether have permission to read or not. Put off to future lab.
     * @param blockNumber     the block number to be read
     * @param position        the position on the block file
     * @param size            read file size
     * @return the buffer that stores the data
     * @throws IllegalArgumentException    if position less than zero, or position+size larger than block size.
     * @throws IllegalAccessTokenException if accessToken is illegal or has no permission on this file
     */
    byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException, IOException;

    /**
     * Write data to a block.
     * It should be redirect to [dataBlockNumber].block file
     *
     * @param fileAccessToken the file accessToken to check whether have permission to write or not. Put off to future lab.
     * @param blockNumber     the block number to be written
     * @param position        the position on the block file
     * @param buffer          the buffer that stores the data
     * @throws IllegalArgumentException    if position less than zero, or position+size larger than block size.
     * @throws IllegalAccessTokenException if accessToken is illegal or has no permission on this file
     */
    void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException, IOException;

    void delete(int blockNumber) throws IOException;

    Exception getException();
}