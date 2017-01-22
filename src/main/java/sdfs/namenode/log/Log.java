package sdfs.namenode.log;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import sdfs.namenode.NameNode;
import sdfs.namenode.NameNodeMetaData;
import sdfs.namenode.NameNodeService;

import java.io.Serializable;

/**
 * Created by pengcheng on 2016/11/15.
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS,include= JsonTypeInfo.As.PROPERTY,property="@class")
@JsonSubTypes({@JsonSubTypes.Type(value=AddBlockLog.class,name="addblock"),@JsonSubTypes.Type(value=MkdirLog.class,name="mkdir")
        ,@JsonSubTypes.Type(value=CopyOnWriteBlockLog.class,name="copyonwrite"),@JsonSubTypes.Type(value=CreateFileLog.class,name="create")
        ,@JsonSubTypes.Type(value=RemoveBlocksLog.class,name="remove"),@JsonSubTypes.Type(value=WriteStartLog.class,name="start")
        ,@JsonSubTypes.Type(value=WriteCommitLog.class,name="commit"),@JsonSubTypes.Type(value=WriteAbortLog.class,name="abort")
        ,@JsonSubTypes.Type(value=RMLog.class,name="rm"),@JsonSubTypes.Type(value=RenameLog.class,name="rename")})
public abstract class Log implements Serializable{
    String name;
    public abstract void redo(NameNodeService nameNodeService, NameNode nameNode, NameNodeMetaData nameNodeMetaData);
}
