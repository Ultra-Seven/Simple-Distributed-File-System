package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.DirNode;
import sdfs.filetree.Entry;
import sdfs.filetree.FileNode;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.util.UUID;

/**
 * Created by lenovo on 2016/11/28.
 */
public class RenameLog extends Log {
    private String fileUri;
    private String newName;
    @JsonCreator
    public RenameLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(String fileUri, String newName) {
        this.fileUri = fileUri;
        this.newName = newName;
    }
    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        FileNode fileNode = nameNodeService.open(fileUri);
        DirNode dirNode = nameNodeService.getParentDir(fileUri);
        if (fileNode != null && dirNode != null) {
            Entry target = dirNode.getEntries().stream().filter(entry -> entry.getId() == fileNode.getId()).findFirst().orElse(null);
            if (target != null) {
                target.setName(newName);
                FileNode node = nameNodeMetaData.getFileTable().get(fileNode.getId());
                node.setAll(fileNode);
            }
        }
    }
}
