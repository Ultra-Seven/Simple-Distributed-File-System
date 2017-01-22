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
public class WriteAbortLog extends Log {
    private UUID fileAccessToken;

    @JsonCreator
    public WriteAbortLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        FileNode fileNode = nameNode.getReadwriteFile().remove(fileAccessToken);
        nameNode.getNewFileBlocks().remove(fileAccessToken);
        nameNode.getConflict().remove(fileNode.getId());
    }
}
