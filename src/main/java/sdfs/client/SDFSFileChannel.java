/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.Constants;
import sdfs.namenode.LocatedBlock;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private final UUID uuid; //File uuid
    private final Map<Integer, LocatedBlock> locatedBlocksCache; //BlockNumber to LocatedBlock cache
    private ArrayList<LocatedBlock> newBlocks = new ArrayList<>(); //add new blocks
    private final boolean isReadOnly;
    private final Map<Integer, byte[]> dataBlocksCache; //BlockNumber to DataBlock cache. byte[] or ByteBuffer are both acceptable.
    private Map<Integer, Character> ageBits;
    private ArrayList<LocatedBlock> queue;
    private  List<Integer> order;
    private int fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file.
    private int fileDataBlockCacheSize;
    private long position;
    private InetSocketAddress nameNodeAddress;
    private boolean isOpen = true;
    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    private Exception exception = null;

    public SDFSFileChannel(UUID uuid, int fileSize, int blockAmount, Map<Integer, LocatedBlock> locatedBlocksCache, boolean isReadOnly, int fileDataBlockCacheSize) {
        this.uuid = uuid;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.locatedBlocksCache = locatedBlocksCache;
        this.fileDataBlockCacheSize = fileDataBlockCacheSize;
        this.isReadOnly = isReadOnly;
        if (locatedBlocksCache != null)
            this.order = new ArrayList<>(locatedBlocksCache.size());
        this.dataBlocksCache = new HashMap<>(fileDataBlockCacheSize);
        this.ageBits = new HashMap<>(fileDataBlockCacheSize);
        this.queue = new ArrayList<>(fileDataBlockCacheSize);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        //todo your code here
        if (isOpen) {
            if (dataBlocksCache.size() == 0) {
                initDataBlocksCache();
            }
            int len = 0;
            //System.out.println("position:" + position + ";fileSize:" + fileSize);
            if (position < fileSize) {
                int i = (int) (position / Constants.DEFAULT_BLOCK_SIZE);
                while (dst.remaining() > 0 && i < order.size()) {
                    LocatedBlock block = locatedBlocksCache.get(order.get(i));
                    int offset = (int) (position % Constants.DEFAULT_BLOCK_SIZE);
                    int size = Math.min(block.getSize() - offset, dst.remaining());
                    //System.out.println("size:" + size + ";offset:" + offset + ";block size:" + block.getSize());
                    if (dataBlocksCache.get(order.get(i)) == null) {
                        LRU(block, null);
                    }
                    //clock algorithm
                    ageBits.put(order.get(i), '1');
                    //LRU algorithm
                    headFirst(block);
                    dst.put(dataBlocksCache.get(order.get(i)), offset, size);
                    //System.out.println(Arrays.toString(dst.array()));
                    i++;
                    position = position + size;
                    len = len + size;
                }
            }
            return len;
        }
        else
            throw new ClosedChannelException();
    }

    private void initDataBlocksCache() {
        order.stream().forEach(e-> {
            if (dataBlocksCache.size() < fileDataBlockCacheSize) {
                LocatedBlock block = locatedBlocksCache.get(e);
                InetSocketAddress inetSocketAddress = new InetSocketAddress(block.inetAddress, block.port);
                DataNodeStub dataNodeStub = new DataNodeStub(inetSocketAddress);
                byte[] writeByte = null;
                try {
                    writeByte = dataNodeStub.read(uuid, block.blockNumber, 0, block.size);
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                dataBlocksCache.put(block.blockNumber, writeByte);
            }
        });
    }
    private void CClock(LocatedBlock block) throws IOException {
        boolean check;
//        while (check = iterator.hasNext()) {
//            int blockNumber = (int) ((Map.Entry) iterator.next()).getKey();
//            if (ageBits.get(blockNumber) == '0') {
//                InetSocketAddress inetSocketAddress = new InetSocketAddress(block.inetAddress, block.port);
//                DataNodeStub dataNodeStub = new DataNodeStub(inetSocketAddress);
//                byte[] writeByte = dataNodeStub.read(uuid, block.blockNumber, 0, block.size);
//                dataBlocksCache.remove(blockNumber);
//                ageBits.remove(blockNumber);
//                dataBlocksCache.put(block.blockNumber, writeByte);
//                ageBits.put(block.blockNumber, '0');
//                break;
//            }
//            else
//                ageBits.put(blockNumber, '0');
//        }
//        if (!check) {
//            iterator = dataBlocksCache.entrySet().iterator();
//            LRU(block);
//        }
    }
    private void LRU(LocatedBlock block, byte[] writeByte) throws IOException {
        if (queue.size() >= fileDataBlockCacheSize) {
            LocatedBlock block1 = queue.remove(queue.size() - 1);
            int blockNumber = block1.blockNumber;
            InetSocketAddress inetSocketAddress = new InetSocketAddress(block1.inetAddress, block1.port);
            DataNodeStub dataNodeStub = new DataNodeStub(inetSocketAddress);
            dataNodeStub.write(uuid, blockNumber, 0, dataBlocksCache.get(blockNumber));
            dataBlocksCache.remove(blockNumber);
        }
        if (writeByte == null) {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(block.inetAddress, block.port);
            DataNodeStub dataNodeStub = new DataNodeStub(inetSocketAddress);
            writeByte = dataNodeStub.read(uuid, block.blockNumber, 0, block.size);
        }
        dataBlocksCache.put(block.blockNumber, writeByte);
    }
    private void headFirst(LocatedBlock block) {
        queue.remove(block);
        queue.add(0, block);
    }
    @Override
    public int write(ByteBuffer src) throws IOException {
        //todo your code here
        if (isOpen) {
            if (dataBlocksCache.size() == 0) {
                initDataBlocksCache();
            }
            if (!isReadOnly) {
                int i = (int) (position / Constants.DEFAULT_BLOCK_SIZE);
                int len = 0;
                if (src.remaining() > 0) {
                    if (position <= fileSize) {
                        len = writeIntoBuffer(i, src);
                    } else {
                        int index = fileSize / Constants.DEFAULT_BLOCK_SIZE;
                        //System.out.println(position);
                        while (position > fileSize) {
                            int offset = fileSize - index * Constants.DEFAULT_BLOCK_SIZE;
                            int size = (int) Math.min(Constants.DEFAULT_BLOCK_SIZE, position - index * Constants.DEFAULT_BLOCK_SIZE);
                            //System.out.println("size:" + size + ";index:" + index + ";offset:" + offset + ";fileSize:" + fileSize + ";order:" + order.size());
                            if (index < order.size()) {
                                LocatedBlock locatedBlock = locatedBlocksCache.get(order.get(index));
                                locatedBlock.setSize(size);
                                fileSize = fileSize + size - offset;
                                byte[] dst = new byte[size];
                                System.arraycopy(dataBlocksCache.get(locatedBlock.blockNumber), 0, dst, 0, offset);
                                for (int k = offset; k < size; k++) {
                                    dst[k] = (byte) 0;
                                }
                                LRU(locatedBlock, dst);
                                headFirst(locatedBlock);
                                index++;
                            }
                            else {
                                //TODO:fill the last block
                                NameNodeStub nameNodeStub = new NameNodeStub(nameNodeAddress, 16);
                                LocatedBlock locatedBlock = nameNodeStub.addBlocks(uuid, 1).get(0);
                                locatedBlock.setSize(size);
                                byte[] dst = new byte[size];
                                for (int m = 0; m < size; m++)
                                    dst[m] = (byte)0;
                                newBlocks.add(locatedBlock);
                                order.add(locatedBlock.blockNumber);
                                locatedBlocksCache.put(locatedBlock.blockNumber, locatedBlock);
                                blockAmount++;
                                fileSize = fileSize + size;
                                LRU(locatedBlock, dst);
                                headFirst(locatedBlock);
                                index++;
                            }
                        }
                        int in = (int) (position / Constants.DEFAULT_BLOCK_SIZE);
                        //System.out.println("in:" + in);
                        len = writeIntoBuffer(in, src);
                    }
                }
                return len;
            }
            else
                throw new NonWritableChannelException();
        }
        else
            throw new ClosedChannelException();
    }

    @Override
    public long position() throws IOException {
        //todo your code here
        if (isOpen)
            return position;
        else
            throw new ClosedChannelException();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        //todo your code here
        if (isOpen) {
            if (newPosition < 0)
                throw new IllegalArgumentException("position can not be less then 0!");
            this.position = newPosition;
            return this;
        }
        else
            throw new ClosedChannelException();
    }

    @Override
    public long size() throws IOException {
        //todo your code here
        if (isOpen)
            return fileSize;
        else
            throw new ClosedChannelException();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        //todo your code here
        if (isOpen) {
            if (!isReadOnly) {
                if (fileSize > size) {
                    int delta = (int) (fileSize - size);
                    fileSize = (int) size;
                    while (delta > 0) {
                        LocatedBlock lastBlock = locatedBlocksCache.get(order.get(order.size() - 1));
                        int removeSize = Math.min(delta, lastBlock.getSize());
                        if (delta < lastBlock.getSize()) {
                            byte[] truncate = new byte[lastBlock.getSize() - delta];
                            if (dataBlocksCache.get(lastBlock.blockNumber) == null)
                                LRU(lastBlock, null);
                            headFirst(lastBlock);
                            lastBlock.setSize(truncate.length);
                            System.arraycopy(dataBlocksCache.get(lastBlock.blockNumber), 0, truncate, 0, truncate.length);
                            dataBlocksCache.put(lastBlock.blockNumber, truncate);
                        } else {
                            locatedBlocksCache.remove(lastBlock.blockNumber);
                            order.remove(order.size() - 1);
                            queue.remove(lastBlock);
                            dataBlocksCache.remove(lastBlock.blockNumber);
                            blockAmount--;
                        }
                        delta = delta - removeSize;
                        if (position > fileSize)
                            position = fileSize;
                    }
                }
                return this;
            }
            else
                throw new NonWritableChannelException();
        }
        else
            throw new ClosedChannelException();
    }

    @Override
    public boolean isOpen() {
        //todo your code here
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        //todo your code here
        if (isOpen) {
            flush();
            this.isOpen = false;

        }

    }

    @Override
    public void flush() throws IOException {
        //todo your code here
        if (isOpen) {
            if(!isReadOnly) {
                Iterator iterator = locatedBlocksCache.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    LocatedBlock block = (LocatedBlock) entry.getValue();
                    InetSocketAddress inetSocketAddress = new InetSocketAddress(block.inetAddress, block.port);
                    DataNodeStub dataNodeStub = new DataNodeStub(inetSocketAddress);
                    dataNodeStub.write(uuid, block.blockNumber, 0, dataBlocksCache.get(block.blockNumber));
                }
                NameNodeStub nameNodeStub = new NameNodeStub(nameNodeAddress, 16);
                ArrayList list = new ArrayList(locatedBlocksCache.size());
                order.stream().forEach(b -> list.add(locatedBlocksCache.get(b)));
                nameNodeStub.setBlocks(uuid, list);
                nameNodeStub.closeReadwriteFile(uuid, fileSize);
            }
            else
                new NameNodeStub(nameNodeAddress, 16).closeReadonlyFile(uuid);
        }
        else
            throw new ClosedChannelException();
    }

    private int writeIntoBuffer(int i, ByteBuffer src) throws IOException {
        int len = 0;
        while (src.remaining() > 0) {
            if (i < order.size()) {
                int blockNumber = order.get(i);
                LocatedBlock block = locatedBlocksCache.get(blockNumber);
                if (dataBlocksCache.get(blockNumber) == null)
                    LRU(block, null);
                //clock algorithm
                ageBits.put(blockNumber, '1');
                //LRU algorithm
                headFirst(block);
                int offset = (int) (position % Constants.DEFAULT_BLOCK_SIZE);
                int size = Math.min(Constants.DEFAULT_BLOCK_SIZE, src.remaining() + offset);
                byte[] dst = new byte[size];
                //System.out.println("offset:" + offset + ";size:" + size);
                System.arraycopy(dataBlocksCache.get(blockNumber), 0, dst, 0, offset);
                for (int m = offset; m < size; m++) {
                    dst[m] = src.get();
                    //System.out.println("read" + dst[m]);
                }
                block.setSize(size);
                dataBlocksCache.put(blockNumber, dst);
                int readByte = size - offset;
                len = len + readByte;
                position = position + readByte;
                i++;
            } else {
                    /*int blockAmount = (src.remaining() % Constants.DEFAULT_BLOCK_SIZE == 0) ? (src.remaining() / Constants.DEFAULT_BLOCK_SIZE)
                            : (src.remaining() / Constants.DEFAULT_BLOCK_SIZE + 1);*/
                //List<LocatedBlock> blocks = nameNodeStub.addBlocks(uuid, blockAmount);
                int size = Math.min(src.remaining(), Constants.DEFAULT_BLOCK_SIZE);
                NameNodeStub nameNodeStub = new NameNodeStub(nameNodeAddress, 16);
                LocatedBlock block = nameNodeStub.addBlocks(uuid, 1).get(0);
                //System.out.println("add local block" + block.blockNumber);
                block.setSize(size);
                locatedBlocksCache.put(block.blockNumber, block);
                dataBlocksCache.put(block.blockNumber, new byte[size]);
                order.add(block.blockNumber);
                newBlocks.add(block);
                blockAmount++;
            }
        }
        if (position > fileSize)
            fileSize = (int) position;

        return len;
    }
    public void setOrder(List<Integer> order) {
        this.order = order;
    }

    public void setBlockAmount(int blockAmount) {
        this.blockAmount = blockAmount;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public void setNameNodeAddress(InetSocketAddress nameNodeAddress) {
        this.nameNodeAddress = nameNodeAddress;
    }

    public int getBlockAmount() {
        return blockAmount;
    }
}
