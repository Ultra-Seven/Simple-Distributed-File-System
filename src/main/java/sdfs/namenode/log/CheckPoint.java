package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

/**
 * Created by lenovo on 2016/11/25.
 */
public class CheckPoint extends Log {

    @JsonCreator
    public CheckPoint(@JsonProperty("name")String name) {
        this.name = name;
    }

    @Override
    public void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData) {

    }
}
