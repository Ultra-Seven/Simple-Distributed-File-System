package sdfs.mapreduce.Task;

import java.io.Serializable;

/**
 * Created by lenovo on 2016/10/7.
 */
public class FileBlock implements Serializable {
    //file name
    private String fileName;
    private int offset;
    private int size;
    public FileBlock(String fileName, int offset, int size) {
        this.fileName = fileName;
        this.offset = offset;
        this.size = size;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
