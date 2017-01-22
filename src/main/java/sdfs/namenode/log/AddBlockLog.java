package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class AddBlockLog extends Log {
    private UUID fileAccessToken;
    private LocatedBlock locatedBlock;
    @JsonCreator
    public AddBlockLog(@JsonProperty("name")String name) {
        this.name = name;
    }
    public void setAll(UUID fileAccessToken, LocatedBlock locatedBlock) {
        this.fileAccessToken = fileAccessToken;
        this.locatedBlock = locatedBlock;
    }

    public UUID getFileAccessToken() {
        return fileAccessToken;
    }

    public void setFileAccessToken(UUID fileAccessToken) {
        this.fileAccessToken = fileAccessToken;
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
            LocatedBlock block = locatedBlock;
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(block);
            fileNode.addBlockInfo(blockInfo);
        }
    }
}
