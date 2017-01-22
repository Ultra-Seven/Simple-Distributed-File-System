/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import sdfs.namenode.LocatedBlock;
import sdfs.namenode.INameNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode{
    /**
     * Read data from a block.
     * It should be redirect to [blockNumber].block file
     *
     * @param blockNumber the block number to be read
     * @param offset      the offset on the block file
     * @param size        the total size to be read
     * @param b           the buffer to store the data
     * @return the total number of bytes read into the buffer
     * @throws IndexOutOfBoundsException if offset less than zero, or offset+size larger than block size, or buffer size is less that size given
     * @throws FileNotFoundException     if the block is free (block file not exist)
     */
    byte[] read(int blockNumber, int offset, int size, byte b[]) throws IndexOutOfBoundsException, FileNotFoundException, IOException, RemoteException;

    /**
     * Write data to a block.
     * It should be redirect to [blockNumber].block file
     *
     * @param blockNumber the block number to be written
     * @param offset      the offset on the block file
     * @param size        the total size to be written
     * @param b           the buffer to store the data
     * @throws IndexOutOfBoundsException  if offset less than zero, or offset+size larger than block size, or buffer size is less that size given
     * @throws FileAlreadyExistsException if the block is not free (block file exist)
     */
    void write(int blockNumber, int offset, int size, byte b[]) throws IndexOutOfBoundsException, FileAlreadyExistsException, IOException, RemoteException;
    /**
     * get the offset of a block.
     *
     * @param block the LocateBlock
     *
     */
    int getOffset(LocatedBlock block) throws RemoteException;
    //send a heart beat to the name Node
    void sendHeartBeat(INameNode name) throws RemoteException;
    //delete a block
    void deleteBlock(int blockNumber) throws RemoteException;

    int getMaxBlockId();
}