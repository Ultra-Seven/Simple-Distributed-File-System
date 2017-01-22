/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.filetree.FileNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface INameNode{
    /**
     * Open a file that is already exist.
     *
     * @param fileUri The file uri to be open
     * @return The file node represent the file
     * @throws FileNotFoundException if the file is not exist
     */
    FileNode open(String fileUri) throws IOException;

    /**
     * Create a empty file.
     *
     * @param fileUri The file uri to be create
     * @return The file node represent the file. It should occupy none blocks.
     * @throws FileAlreadyExistsException if the file is already exist
     */
    FileNode create(String fileUri) throws IOException, RemoteException;

    /**
     * Close a file. If file metadata is changed, store them on the disk.
     *
     * @param fileUri file to be closed
     */
    void close(String fileUri) throws IOException;

    /**
     * Make a directory on given file uri.
     *
     * @param fileUri the directory path
     * @throws FileAlreadyExistsException if directory or file is already exist
     */
    void mkdir(String fileUri) throws SDFSFileAlreadyExistException;

    /**
     * Request a new free block for a file
     * No metadata should be written to disk until it is correctly close
     *
     * @param fileUri the file that request the new block
     * @return a block that is free and could be used by client
     */
    LocatedBlock addBlock(String fileUri);
    //get all blocks of the file
    ArrayList<LocatedBlock> getBlockLocations(String fileUri);
    //save all meta data
    void saveMetaData();
    //update all blocks in the name node
    void updateBlock(String fileUri, ArrayList<LocatedBlock> locatedBlocks);
    //check if the data node is alive
    void checkBlock(int block, String slave);
    //remove the file
    void rm(String fileUri) throws FileNotFoundException;

}
