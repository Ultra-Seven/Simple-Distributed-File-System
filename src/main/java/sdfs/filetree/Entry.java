/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.Serializable;

public class Entry implements Serializable {
    private final Node node;
    private String name;
    private static final long serialVersionUID = 1L;
    private char fileType;
    private int fileNameLength;
    private int id = -1;

    public Entry(String name, Node node) {
        if (name == null || node == null) {
            throw new NullPointerException();
        }
        if (name.isEmpty() || name.contains("/")) {
            throw new IllegalArgumentException();
        }
        this.name = name;
        this.node = node;
    }

    public void setName(String name) {
        if (name == null) {
            throw new NullPointerException();
        }
        if (name.isEmpty() || name.contains("/")) {
            throw new IllegalArgumentException();
        }
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entry entry = (Entry) o;

        return name.equals(entry.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public String getName() {
        return name;
    }

    public Node getNode() {
        return node;
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
        return "Entry" + ";type:" + (getFileType() == '0' ? "FILE" : "DIR") + ";name:" + getName() + ";id:" + getId();
    }
}
