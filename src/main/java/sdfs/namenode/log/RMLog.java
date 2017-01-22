package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.DirNode;
import sdfs.filetree.FileNode;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.rmi.RemoteException;

/**
 * Created by lenovo on 2016/11/28.
 */
public class RMLog extends Log {
    String fileUri;
    @JsonCreator
    public RMLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(String fileUrl) {
        this.fileUri = fileUrl;
    }
    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {
        String[] directories = fileUri.split("/");
        DirNode p = nameNodeService.getParentDir(fileUri);
        if(p != null) {
            int id = p.findFile(directories[directories.length - 1]);
            FileNode fileNode = nameNodeService.open(fileUri);
            if (fileNode != null) {
                try {
                    nameNode.removeLastBlocks(fileNode.getFileUuid(), fileNode.getBlockNum());
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                p.remove(fileNode);
                nameNodeMetaData.getFileTable().remove(id);
                nameNodeMetaData.getFileUuidTable().remove(fileNode.getFileUuid());
            }
        }
    }
}
