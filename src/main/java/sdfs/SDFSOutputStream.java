/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs;

import sdfs.client.DFSClient;
import sdfs.filetree.FileNode;
import sdfs.namenode.INameNode;
import sdfs.namenode.LocatedBlock;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.NotBoundException;
import java.util.ArrayList;

public class SDFSOutputStream implements Closeable, Flushable {
    private String fileUri;
    private FileNode fileNode;
    private INameNode service;
    private ByteBuffer lineBuffer;
    private DFSClient client;
    private int size;
    private byte[] writeByte;
    public SDFSOutputStream() {
        fileUri = "";
    }
    public SDFSOutputStream(String fileUri, INameNode service, DFSClient client) {
        this.fileUri = fileUri;
        this.service = service;
        this.client = client;
    }
    public SDFSOutputStream(SDFSOutputStream outputStream) throws IOException {
        this(outputStream.fileUri, outputStream.service, outputStream.client);
        fileNode = outputStream.service.create(fileUri);
    }
    public void write(byte[] b) throws IOException {
        //todo your code here
        if (fileNode != null) {
            int old = 0;
            if (writeByte == null)
                writeByte = new byte[b.length];
            else {
                old = writeByte.length;
                writeByte = new byte[b.length + old];
            }
            System.arraycopy(b, 0, writeByte, old, b.length);
            lineBuffer = ByteBuffer.wrap(writeByte);
            size = size + b.length;
        }
        else
            System.out.println("Wrong path!");
    }

    @Override
    public void flush() throws IOException {
        //todo your code here
        //System.out.println("size " + size);
        if(size > 0){
            ArrayList<LocatedBlock> blocks = client.createBlocks(fileUri, size);
            blocks.stream().forEach(e->System.out.println(e.blockNumber));
            if (blocks != null) {
                if (blocks.size() == 0) {
                    throw new IOException("can't allocate new block!");
                }
                try {
                    ArrayList<LocatedBlock> update = client.writeBlock(blocks, lineBuffer);
                    client.updateBlock(fileUri, update);
                    System.out.println("write successfully!");
                } catch (NotBoundException e) {
                    e.printStackTrace();
                }
                lineBuffer.clear();
            }
            else
                System.out.println("The path is wrong!");
        }
    }

    @Override
    public void close() throws IOException {
        //todo your code here
        flush();
    }
}
