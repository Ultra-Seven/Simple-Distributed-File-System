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
public class WriteStartLog extends Log {
    private String fileUri;
    private UUID fileAccessToken;

    @JsonCreator
    public WriteStartLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(String fileUri, UUID fileAccessToken) {
        this.fileUri = fileUri;
        this.fileAccessToken = fileAccessToken;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
    }

    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        FileNode fileNode = nameNodeService.open(fileUri);
        nameNode.getReadwriteFile().putIfAbsent(fileAccessToken, fileNode);
        nameNode.getConflict().put(fileNode.getId(), fileAccessToken);
    }
}
