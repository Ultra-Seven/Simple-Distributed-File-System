package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.FileNode;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class RemoveBlocksLog extends Log {
    private UUID fileAccessToken;
    private int blockAmount;

    @JsonCreator
    public RemoveBlocksLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(UUID fileAccessToken, int blockAmount) {
        this.fileAccessToken = fileAccessToken;
        this.blockAmount = blockAmount;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public int getBlockAmount() {
        return blockAmount;
    }

    public void setBlockAmount(int blockAmount) {
        this.blockAmount = blockAmount;
    }

    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        if (blockAmount > 0) {
            FileNode fileNode = nameNode.getReadwriteFile().get(fileAccessToken);
            for (int i = 0; i < blockAmount; i++) {
                if (fileNode != null) {
                    if (fileNode.getBlockNum() > 0) {
                        fileNode.removeLastBlock();
                    }
                }
            }
        }
    }
}
