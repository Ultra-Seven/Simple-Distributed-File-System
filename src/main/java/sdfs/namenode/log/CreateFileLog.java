package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.DirNode;
import sdfs.filetree.FileNode;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class CreateFileLog extends Log {
    private String fileUri;
    private UUID fileAccessToken;

    @JsonCreator
    public CreateFileLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll (String fileUri, UUID fileAccessToken) {
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
        String[] directories = fileUri.split("/");
        String name = directories[directories.length - 1];
        DirNode node = directories.length > 1 ? nameNodeService.getParentDir(fileUri) : nameNodeMetaData.getRoot();
        if (node != null) {
            FileNode fileNode = nameNodeMetaData.getFileTable().get(node.findFile(name));
            DirNode dirNode = nameNodeMetaData.getDirTable().get(node.findDir(name));
            if (fileNode == null && dirNode == null) {
                FileNode fileNode2 = node.createFile(name, nameNodeMetaData);
                FileNode newFileNode = new FileNode();
                newFileNode.setAll(fileNode2);
            }
            else if (dirNode == null){
                FileNode newFileNode = new FileNode();
                newFileNode.setAll(fileNode);
                nameNode.getReadwriteFile().putIfAbsent(fileAccessToken, newFileNode);
            }
        }
        else
            System.out.println("wrong");
    }
}
