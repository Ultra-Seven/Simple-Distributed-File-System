package sdfs.filetree;

import java.io.Serializable;

/**
 * Created by lenovo on 2016/10/1.
 */
public class MasterEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private char fileType;
    private int fileNameLength;
    private String fileName;
    private int id = -1;
    public MasterEntry() {

    }
    public MasterEntry(char fileType, int fileNameLength, String fileName, int id) {
        this.fileType = fileType;
        this.fileNameLength = fileNameLength;
        this.fileName = fileName;
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getFileNameLength() {
        return fileNameLength;
    }

    public void setFileNameLength(int fileNameLength) {
        this.fileNameLength = fileNameLength;
    }

    public char getFileType() {
        return fileType;
    }

    public void setFileType(char fileType) {
        this.fileType = fileType;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getState() {
        return "Entry" + ";type:" + (getFileType() == '0' ? "FILE" : "DIR") + ";name:" + getFileName() + ";id:" + getId();
    }
}
