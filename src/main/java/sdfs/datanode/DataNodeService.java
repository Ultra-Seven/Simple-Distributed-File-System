package sdfs.datanode;

import sdfs.namenode.LocatedBlock;
import sdfs.namenode.INameNode;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

/**
 * Created by lenovo on 2016/10/1.
 * DataNode service implementing IDataNode interface
 */
public class DataNodeService implements IDataNode {
    private String dataDir = "data";
    private static final String prefix  = ".block";
    private SDFSSlave slave;
    public DataNodeService() {

    }
    public DataNodeService(SDFSSlave slave) throws RemoteException {
        super();
        this.slave = slave;
    }
    @Override
    public byte[] read(int blockNumber, int offset, int size, byte buffer[]) throws IndexOutOfBoundsException, IOException {
        File file = new File(getFilePath(blockNumber));
        byte[] read = new byte[size];
        if(file.exists()){
            FileInputStream fileInputStream = new FileInputStream(file);
            int len = fileInputStream.read(read, offset, size);
            for (int i = 0; i < size; i++)
                buffer[i] = read[i];
//            for (int i = 0; i < size; i++)
//                System.out.println(buffer[i]);
            if(len > 0){
                fileInputStream.close();
                return read;
            }
        }
        return null;
    }

    @Override
    public void write(int blockNumber, int offset, int size, byte[] b) throws IndexOutOfBoundsException, IOException {
        String path = getFilePath(blockNumber);
        File file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
        }
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
        os.write(b, offset, size);
        os.close();
    }

    @Override
    public int getOffset(LocatedBlock block) {
        File file = new File(getFilePath(block.blockNumber));
        if(file.exists()){
            return (int) file.length();
        }
        return 0;
    }

    @Override
    public void sendHeartBeat(INameNode name) {
        File file=new File("data");
        File[] tempList = file.listFiles();
        ArrayList<Integer> ids = new ArrayList<>();
        for (int i = 0; i < tempList.length; i++) {
            int nameId = getNameID(tempList[i].getName());
            ids.add(nameId);
        }
        ids.stream().forEach(e -> {
            name.checkBlock(e, slave.toString());
        });
    }
    //parse the file name and get the block id
    private int getNameID(String name) {
        String id = "";
        for (int i = 0; i < name.length(); i++) {
            if (name.charAt(i) == '.')
                break;
            id = id + name.charAt(i);
        }
        return Integer.parseInt(id);
    }

    @Override
    public void deleteBlock(int blockNumber) {
        String path = getFilePath(blockNumber);
        //System.out.println(path);
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Override
    public int getMaxBlockId()  {
        File file=new File("data");
        File[] tempList = file.listFiles();
        int max = 0;
        for (int i = 0; i < tempList.length; i++) {
            int nameId = getNameID(tempList[i].getName());
            if (nameId >= max)
                max = nameId + 1;
        }
        return max;
    }

    //get the specific path of a block
    private String getFilePath(int blockNumber) {
        return dataDir + System.getProperty("file.separator") + blockNumber + prefix;
    }

    public void removeFile(Integer integer) {
        File file = new File("data/" + integer + ".block");
        if (file.exists()) {
            file.delete();
        }
    }
}
