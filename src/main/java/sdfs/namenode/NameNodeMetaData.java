package sdfs.namenode;

import sdfs.Constants;
import sdfs.filetree.DirNode;
import sdfs.filetree.FileNode;
import sdfs.filetree.MasterEntry;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lenovo on 2016/10/1.
 * name node
 * save all metadata and namespace
 */
public class NameNodeMetaData implements Serializable {
    private static final long serialVersionUID = 1L;
    //the map of all directories
    private Map<Integer, DirNode> dirTable;
    //the map of all files
    private Map<Integer, FileNode> fileTable;
    //the map of all files
    private Map<UUID, FileNode> fileUuidTable;
    //the map of all data node
    private Map<String, Boolean> slaveBooleanMap;

    private final Map<UUID, FileNode> readonlyFile = new ConcurrentHashMap<>();
    private final Map<UUID, FileNode> readwriteFile = new ConcurrentHashMap<>();

    private static NameNodeMetaData nameNode;
    public DirNode root;
    public static int maxId;
    NameNodeMetaData() {
        String path = System.getProperty("sdfs.namenode.dir") == null ? Constants.META_DATA_PATH : System.getProperty("sdfs.namenode.dir") + "/root.node";
        File file = new File(path);
        NameNodeMetaData temp = null;
        if (file.exists()) {
            ObjectInputStream is;
            try {
                is = new ObjectInputStream(new FileInputStream(path));
                temp = (NameNodeMetaData) is.readObject();
                is.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            if (temp != null) {
                this.dirTable = temp.getDirTable();
                this.fileTable = temp.getFileTable();
                this.slaveBooleanMap = temp.getSlaveBooleanMap();
                this.fileUuidTable = temp.getFileUuidTable();
                this.root = temp.getRoot();
            }
        }
        else {
            dirTable = new ConcurrentHashMap<>();
            fileTable = new ConcurrentHashMap<>();
            slaveBooleanMap = new ConcurrentHashMap<>();
            fileUuidTable = new ConcurrentHashMap<>();
            root = new DirNode();
            dirTable.put(0, root);
        }
        for (Object o1 : dirTable.entrySet()) {
            Map.Entry entry = (Map.Entry) o1;
            Integer key = (Integer) entry.getKey();
            maxId = Math.max(maxId, key + 1);
        }
        //TODO:overlapping
        saveNameNode(this);
    }

    public Map<Integer, DirNode> getDirTable() {
        return dirTable;
    }

    public static NameNodeMetaData getNameNode() {
        String path = System.getProperty("sdfs.namenode.dir") == null ? Constants.META_DATA_PATH : System.getProperty("sdfs.namenode.dir") + "/root.node";
        if (nameNode == null) {
            try {
                File file = new File(path);
                NameNodeMetaData temp;
                if (file.exists()) {
                    ObjectInputStream is = new ObjectInputStream(new FileInputStream(path));
                    temp = (NameNodeMetaData) is.readObject();
                    nameNode = temp;
                    is.close();
                    nameNode.root = nameNode.dirTable.get(0);
                }
                else {
                    nameNode = new NameNodeMetaData();
                    nameNode.root = new DirNode();
                    nameNode.dirTable.put(0, nameNode.root);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("This is the first running!");
            }
        }
        for (Object o1 : nameNode.dirTable.entrySet()) {
            Map.Entry entry = (Map.Entry) o1;
            Integer key = (Integer) entry.getKey();
            maxId = Math.max(maxId, key + 1);
        }
        return nameNode;
    }
    //read dirNode information from the disk and initiate it
    private static void setDirNode() throws IOException {
        File file=new File("dir");
        File[] tempList = file.listFiles();
        Map<Integer, DirNode> dirTable = new HashMap<>();
        for (File aTempList : tempList) {
            FileInputStream fis = new FileInputStream(aTempList);
            int nameId = getNameID(aTempList.getName());
            ArrayList<MasterEntry> masterEntries = new ArrayList<>();
            while (true) {
                int first;
                if (-1 == (first = fis.read()))
                    break;
                char fileType = (char) first;
                int fileLen = fis.read() + (fis.read() << 8) + (fis.read() << 16);
                String fileName = "";
                for (int j = 0; j < fileLen; j++) {
                    fileName = fileName + ((char) fis.read());
                }
                int fileId = fis.read() + (fis.read() << 8) + (fis.read() << 16) + (fis.read() << 24);
                MasterEntry masterEntry = new MasterEntry(fileType, fileLen, fileName, fileId);
                masterEntries.add(masterEntry);
                if (fileId >= maxId) {
                    maxId = fileId;
                    maxId++;
                }
            }
            fis.close();
        }
        if (!dirTable.containsKey(0)) {
            nameNode.root = new DirNode();
            dirTable.put(0, nameNode.root);
        }
        nameNode.setDirTable(dirTable);
    }

    private static int getNameID(String name) {
        String id = "";
        for (int i = 0; i < name.length(); i++) {
            if (name.charAt(i) == '.')
                break;
            id = id + name.charAt(i);
        }
        return Integer.parseInt(id);
    }
    //read fileNode information from the disk and initiate it
    private static void setFileNode() throws IOException, ClassNotFoundException {
        File file = new File(Constants.META_DATA_PATH);
        Map<Integer, FileNode> temp;
        if (file.exists()) {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(Constants.META_DATA_PATH));
            temp = (Map<Integer, FileNode>) is.readObject();
            nameNode.setFileTable(temp);
            is.close();
        }
    }

    public DirNode getRoot() {
        return root;
    }

    public Map<Integer, FileNode> getFileTable() {
        return fileTable;
    }
    //print state of name node
    public static void printState(NameNodeMetaData nameNode) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(new Date()));
        System.out.println("==========dirTable==========");
        for (Object o1 : nameNode.dirTable.entrySet()) {
            Map.Entry entry = (Map.Entry) o1;
            Integer key = (Integer) entry.getKey();
            DirNode val = (DirNode) entry.getValue();
            System.out.println(key + ":");
            val.getEntries().forEach(e -> System.out.println(e.getState()));
        }
        System.out.println("==========FileNode==========");
        for (Object o : nameNode.fileTable.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Integer key = (Integer) entry.getKey();
            FileNode val = (FileNode) entry.getValue();
            System.out.println("FIlE id:" + key + ";block num:" + val.getBlockNum() + ";file size:" + val.getFileSize());
        }
    }
    //save all metadata into the disk
    public void saveNameNode() {
        String path = System.getProperty("sdfs.namenode.dir") == null ? Constants.META_DATA_PATH : System.getProperty("sdfs.namenode.dir") + "/root.node";
        try {
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileOutputStream outStream = new FileOutputStream(file.getAbsoluteFile());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);
                objectOutputStream.writeObject(this);
            objectOutputStream.close();
            outStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //printState(this);
    }
    public void saveNameNode(NameNodeMetaData nameNodeMetaData) {
        saveNameNode();
        printState(nameNodeMetaData);
    }

    public void setDirTable(Map<Integer, DirNode> dirTable) {
        this.dirTable = dirTable;
    }

    public void setFileTable(Map<Integer, FileNode> fileTable) {
        this.fileTable = fileTable;
    }

    public Map<String, Boolean> getSlaveBooleanMap() {
        return slaveBooleanMap;
    }

    public Map<UUID, FileNode> getFileUuidTable() {
        return fileUuidTable;
    }

    public void setFileUuidTable(Map<UUID, FileNode> fileUuidTable) {
        this.fileUuidTable = fileUuidTable;
    }

    public Map<UUID, FileNode> getReadonlyFile() {
        return readonlyFile;
    }

    public Map<UUID, FileNode> getReadwriteFile() {
        return readwriteFile;
    }
}
