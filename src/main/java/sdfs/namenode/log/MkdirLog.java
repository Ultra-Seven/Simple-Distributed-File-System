package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.DirNode;
import sdfs.filetree.FileNode;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class MkdirLog extends Log {
    private String fileUri;
    @JsonCreator
    public MkdirLog(@JsonProperty("name") String name) {
        this.name = name;
    }
    public void setAll(String fileUri) {
        this.fileUri = fileUri;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }

    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        String[] directories = fileUri.split("/");
        DirNode node = nameNodeService.getParentDir(fileUri);
        if(node != null) {
            int id = node.findDir(directories[directories.length - 1]);
            DirNode dirNode = nameNodeMetaData.getDirTable().get(id);
            FileNode fileNode = nameNodeMetaData.getFileTable().get(node.findFile(directories[directories.length - 1]));
            if (dirNode == null && fileNode == null) {
                DirNode newNode = node.createDir(directories[directories.length - 1]);
                nameNodeMetaData.getDirTable().putIfAbsent(newNode.getId(), newNode);
            }
        }
    }
}
