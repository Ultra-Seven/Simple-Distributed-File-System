package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.FileNode;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class CopyOnWriteBlockLog extends Log {
    private UUID fileAccessToken;
    private int fileBlockNumber;
    private LocatedBlock locatedBlock;
    @JsonCreator
    public CopyOnWriteBlockLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(UUID fileAccessToken, int fileBlockNumber, LocatedBlock locatedBlock) {
        this.fileAccessToken = fileAccessToken;
        this.fileBlockNumber = fileBlockNumber;
        this.locatedBlock = locatedBlock;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public int getFileBlockNumber() {
        return fileBlockNumber;
    }

    public void setFileBlockNumber(int fileBlockNumber) {
        this.fileBlockNumber = fileBlockNumber;
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }

    public void setLocatedBlock(LocatedBlock locatedBlock) {
        this.locatedBlock = locatedBlock;
    }

    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        FileNode fileNode = nameNode.getReadwriteFile().get(fileAccessToken);
        if (fileNode != null) {
            LocatedBlock block = fileNode.getBlockInfos().get(fileBlockNumber).getBlock();
            int blockNumber = block.blockNumber;
            LocatedBlock newFileBlock = locatedBlock;
            Map blocks = nameNode.getNewFileBlocks().get(fileAccessToken);
            if (blocks == null) {
                blocks = new ConcurrentHashMap();
                nameNode.getNewFileBlocks().put(fileAccessToken, blocks);
            }
            blocks.putIfAbsent(blockNumber, newFileBlock.blockNumber);
        }
    }
}
