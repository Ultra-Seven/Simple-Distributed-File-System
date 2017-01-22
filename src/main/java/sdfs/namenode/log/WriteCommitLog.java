package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.FileNode;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.util.Map;
import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class WriteCommitLog extends Log {
    private UUID fileAccessToken;
    private long newFileSize;

    @JsonCreator
    public WriteCommitLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(UUID fileAccessToken, long newFileSize) {
        this.fileAccessToken = fileAccessToken;
        this.newFileSize = newFileSize;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public long getNewFileSize() {
        return newFileSize;
    }

    public void setNewFileSize(long newFileSize) {
        this.newFileSize = newFileSize;
    }

    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        FileNode fileNode = nameNode.getReadwriteFile().get(fileAccessToken);
        fileNode.setFileSize((int)newFileSize);
        FileNode node = nameNodeMetaData.getFileTable().get(fileNode.getId());
        Map<Integer, Integer> map = nameNode.getNewFileBlocks().get(fileAccessToken);
        if (map != null) {
            map.entrySet().forEach(entry -> fileNode.getBlock(entry.getKey()).setBlockNumber(entry.getValue()));
            nameNode.getNewFileBlocks().remove(fileAccessToken);
        }
        if (node != null)
            node.setAll(fileNode);
        else
            nameNodeMetaData.getFileTable().put(fileNode.getId(), fileNode);
        nameNode.getReadwriteFile().remove(fileAccessToken);
        nameNode.getConflict().remove(fileNode.getId());
    }
}
