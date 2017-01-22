/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import sdfs.Constants;
import sdfs.client.DataNodeStub;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.*;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo> {
    private static final long serialVersionUID = -5007570814999866661L;
    private final List<BlockInfo> blockInfos = new ArrayList<>();
    private int fileSize;//file size should be checked when closing the file.
    private int id;
    private UUID fileUuid;
    private List<Integer> order = new ArrayList<>();
    public static int maxId = 0;

    public FileNode() {
        id = NameNodeMetaData.maxId ;
        NameNodeMetaData.maxId = id + 1;
        fileUuid = UUID.randomUUID();
    }
    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfos.add(blockInfo);
    }
    public LocatedBlock getBlock(int blockNumber) {
        BlockInfo blockInfor = blockInfos.stream().filter(locatedBlocks ->
                locatedBlocks.getLocatedBlocks().get(0).blockNumber == blockNumber).findFirst().orElse(null);
        if (blockInfor != null) {
            return blockInfor.getBlock();
        }
        return null;
    }
    public void removeLastBlockInfo() {
        blockInfos.remove(blockInfos.size() - 1);
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
        for (int i = 0; i < blockInfos.size(); i++) {
            int size = Math.min(fileSize, Constants.DEFAULT_BLOCK_SIZE);
            blockInfos.get(i).getLocatedBlocks().stream().forEach(locatedBlock -> locatedBlock.setSize(size));
            fileSize = fileSize - size;
            if (fileSize <= 0)
                break;
        }
    }
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfos.listIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfos.equals(that.blockInfos);
    }

    @Override
    public int hashCode() {
        return blockInfos.hashCode();
    }

    public UUID getFileUuid() {
        return fileUuid;
    }

    public int getBlockNum() {
        return blockInfos.size();
    }

    public void removeLastBlock() {
//        blockInfos.get(blockInfos.size() - 1).getLocatedBlocks().stream().forEach(block -> {
//            InetSocketAddress inetSocketAddress = new InetSocketAddress(block.inetAddress, block.port);
//            DataNodeStub dataNodeStub = new DataNodeStub(inetSocketAddress);
//            try {
//                dataNodeStub.delete(block.blockNumber);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
        blockInfos.remove(blockInfos.size() - 1);
    }

    public void removeAll() {
        while (blockInfos.size() != 0) {
            blockInfos.remove(0);
        }
    }

    public Map<Integer,LocatedBlock> getMap() {
        Map<Integer,LocatedBlock> map = new HashMap<>(blockInfos.size());
        order = new ArrayList<>(blockInfos.size());
        blockInfos.stream().forEach(locatedBlocks ->{
            LocatedBlock block = locatedBlocks.getBlock();;
            map.put(block.blockNumber, block);
            order.add(block.blockNumber);
        });
        return map;
    }

    public List<Integer> getOrder() {
        return order;
    }

    public void setAll(FileNode fileNode) {
        this.id = fileNode.getId();
        this.fileSize = fileNode.getFileSize();
        order = new ArrayList<>();
        removeAll();
        fileNode.getOrder().forEach(integer -> this.order.add(integer));
        Collections.addAll(blockInfos,  new BlockInfo[fileNode.blockInfos.size()]);
        Collections.copy(blockInfos, fileNode.blockInfos);
    }

    public List<BlockInfo> getBlockInfos() {
        return blockInfos;
    }

}

