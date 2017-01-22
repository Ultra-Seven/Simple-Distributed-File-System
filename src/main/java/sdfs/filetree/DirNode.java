/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;
import sdfs.namenode.NameNodeMetaData;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirNode extends Node implements Serializable, Iterable<Entry>  {
    //todo your code here
    //the list of entries
    private static final long serialVersionUID = 8178778592344231767L;
    private final Set<Entry> entries = new HashSet<>();
    //dir node id
    private int id;
    public DirNode() {
        id = NameNodeMetaData.maxId ;
        //System.out.println("maxId" + id);
        NameNodeMetaData.maxId = id + 1;
    }
    public DirNode(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
    //find the id of a file/dir in the directory
    public int findPredecessorId(String fileName) {
        Entry entry = entries.stream().filter(e->e.getName().equals(fileName)).findFirst().orElse(null);
        if (entry != null)
            return entry.getId();
        return -1;
    }
    //find the file in the dir
    public int findFile(String fileName) {
        Entry entry = entries.stream().filter(e->e.getName().equals(fileName) && e.getFileType() == '0').findFirst().orElse(null);
        if (entry != null)
            return entry.getId();
        return -1;
    }
    //find the dir in the dir
    public int findDir(String dirName) {
        Entry entry = entries.stream().filter(e->e.getName().equals(dirName) && e.getFileType() == '1').findFirst().orElse(null);
        if (entry != null)
            return entry.getId();
        return -1;
    }
    //create a file
    public FileNode createFile(String fileName, NameNodeMetaData nameNodeMetaData) {
        FileNode fileNode = new FileNode();
        Entry entry = new Entry(fileName, fileNode);
        entry.setFileType('0');
        entry.setId(fileNode.getId());
        entries.add(entry);
        nameNodeMetaData.getFileTable().put(fileNode.getId(), fileNode);
        return fileNode;
    }
    //create a directory
    public DirNode createDir(String dirName) {
        DirNode dirNode = new DirNode();
        Entry entry = new Entry(dirName, dirNode);
        entry.setFileType('1');
        entry.setId(dirNode.getId());
        entries.add(entry);
        return dirNode;
    }
    //remove a file
    public void remove(FileNode fileNode) {
        //masterEntries.remove(masterEntries.stream().filter(e->e.getId() == fileNode.getId()).findFirst().orElse(new MasterEntry()));
        Entry entrys = entries.stream().filter(entry -> entry.getId()==(fileNode.getId())).findFirst().orElse(null);
        if (entrys != null)
            entries.remove(entrys);
    }

//    public ArrayList<MasterEntry> getMasterEntries() {
//        return masterEntries;
//    }
    //save all entries in the directory
    public void saveDir() throws IOException {
        String filePath = "dir/" + id + ".node";
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream fos = new FileOutputStream(file);
        entries.forEach(entry -> {
            try {
                fos.write(getEntry(entry));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        fos.close();
    }
    //parse the entry
    private byte[] getEntry(Entry masterEntry) {
        String fileName = masterEntry.getName();
        int len = fileName.length();
        int fileId = masterEntry.getId();
        byte[] entry = new byte[8 + len];
        entry[0] = (byte) masterEntry.getFileType();
        entry[1] = (byte) (len & 0xff);
        entry[2] = (byte) ((len >> 8) & 0xff);
        entry[3] = (byte) ((len >> 16) & 0xff);
        for (int i = 4; i < len + 4; i++)
            entry[i] = (byte) fileName.charAt(i - 4);
        entry[4 + len] = (byte) (fileId & 0xff);;
        entry[5 + len] = (byte) ((fileId >> 8) & 0xff);
        entry[6 + len] = (byte) ((fileId >> 16) & 0xff);
        entry[7 + len] = (byte) (fileId >> 24);
//        for (int i = 0; i < entry.length; i++) {
//            System.out.println(entry[i]);
//        }
        return entry;
    }

    @Override
    public Iterator<Entry> iterator() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DirNode entries1 = (DirNode) o;

        return entries.equals(entries1.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public boolean addEntry(Entry entry) {
        return entries.add(entry);
    }

    public boolean removeEntry(Entry entry) {
        return entries.remove(entry);
    }

    public Set<Entry> getEntries() {
        return entries;
    }
}
