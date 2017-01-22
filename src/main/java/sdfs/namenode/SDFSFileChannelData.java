package sdfs.namenode;

import sdfs.filetree.FileNode;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class SDFSFileChannelData implements Serializable {
    private static final long serialVersionUID = 5725498307666004432L;

    private UUID accessToken;
    private FileNode fileNode;
    private int fileDataBlockCacheSize;

    public SDFSFileChannelData(UUID accessToken, FileNode fileNode, int fileDataBlockCacheSize) {
        this.accessToken = accessToken;
        this.fileNode = fileNode;
        this.fileDataBlockCacheSize = fileDataBlockCacheSize;
    }

    public UUID getAccessToken() {
        return accessToken;
    }

    public FileNode getFileNode() {
        return fileNode;
    }

    public int getFileDataBlockCacheSize() {
        return fileDataBlockCacheSize;
    }
}
