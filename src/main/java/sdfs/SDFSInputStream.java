/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs;

import sdfs.client.DFSClient;
import sdfs.namenode.INameNode;
import sdfs.namenode.LocatedBlock;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.ArrayList;

public class SDFSInputStream implements Closeable {
    private String path;
    private int bufferSize;
    private ArrayList<LocatedBlock> locatedBlocks;
    private INameNode service;
    private int position = 0;
    private DFSClient client;
    public SDFSInputStream(String path, INameNode service, DFSClient client) throws IOException {
        this.path = path;
        this.service = service;
        this.client = client;
    }

    public SDFSInputStream(SDFSInputStream open) {
        this.path = open.path;
        this.bufferSize = open.bufferSize;
        this.locatedBlocks = open.locatedBlocks;
        this.service = open.service;
        this.client = open.client;
    }

    //modified interface
    public int read(int position, int offset, int size, byte[] b) throws IOException, NotBoundException {
        //todo your code here
        this.locatedBlocks = service.getBlockLocations(path);
        if (locatedBlocks != null) {
            long fileLen = getFileLength();
            int fileEnd = (int) Math.min(position + size, fileLen);
            int bufferEnd = Math.min(offset + size, b.length);
            int readLen = Math.min(fileEnd - position, bufferEnd - offset);
            if (readLen > 0) {
                ArrayList<LocatedBlock> blockRange = getBlockRange(position, fileLen);
                long remain = readLen;
                for (LocatedBlock block : blockRange) {
                    long start = position % Constants.DEFAULT_BLOCK_SIZE;
                    int readByte = (int) Math.min(remain, block.getSize() - start);
                    byte[] buffer = new byte[readByte];
                    //int length = client.ReadBlockByte(block, start, start + readByte - 1, buffer);
                    int length = client.ReadBlockByte(block, start, start + readByte - 1, buffer);
                    System.arraycopy(buffer, 0, b, offset, length);
                    position = position + readByte;
                    remain = remain - readByte;
                    offset = offset + readByte;
                }
                return readLen;
            }
        }
        return -1;
    }

    private long getFileLength() {
        return locatedBlocks.stream().map(LocatedBlock::getSize).reduce((a, b) -> (a + b)).orElseGet(() -> 0);
    }

    private ArrayList<LocatedBlock> getBlockRange(int offset, long fileLen) {
        ArrayList<LocatedBlock> blocks = new ArrayList<>();
        int blockIndex = offset / Constants.DEFAULT_BLOCK_SIZE;
        long remain = fileLen;
        while (remain > 0) {
            LocatedBlock block = locatedBlocks.get(blockIndex);
            blocks.add(block);
            assert block != null;
            if (block.getSize() <= remain) {
                remain = remain - block.getSize();
                blockIndex++;
            }
            else {
                break;
            }
        }
        return blocks;
    }

    @Override
    public void close() throws IOException {
        //todo your code here
        service.saveMetaData();
    }

    public void seek(int newPos) throws IndexOutOfBoundsException, IOException {
        //todo your code here
        if (newPos >=0 && newPos < getFileLength()) {
            position = newPos;
        }
        else {
            throw new IndexOutOfBoundsException();
        }
    }
    public int getPosition() {
        return position;
    }
}
