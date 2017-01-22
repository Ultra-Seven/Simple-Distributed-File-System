package sdfs.client;

import sdfs.Constants;
import sdfs.SDFSInputStream;
import sdfs.SDFSOutputStream;
import sdfs.datanode.IDataNode;
import sdfs.filetree.FileNode;
import sdfs.namenode.INameNode;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannelData;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;

import static sdfs.namenode.NameNode.NAME_NODE_IP;
import static sdfs.namenode.NameNode.NAME_NODE_PORT;

/**
 * Created by lenovo on 2016/10/1.
 * Client class which is responsible for build a connection
 * and specific client operations
 */
public class DFSClient implements ISDFSClient{
    //node service
    private INameNode nameNodeService;
    //host ip and port
    private String masterRegistryHost = Constants.DEFAULT_IP;
    private int masterRegistryPort = Constants.DEFAULT_PORT;
    private NameNodeStub nameNodeStub;
    private Socket socket;
    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;
    public DFSClient(String masterRegistryHost, int masterRegistryPort) {
        this.masterRegistryHost = masterRegistryHost;
        this.masterRegistryPort = masterRegistryPort;
    }
    //connect with name node and initiate the name node service
    public void connect() throws RemoteException, NotBoundException, MalformedURLException {
        String path = "rmi://" + masterRegistryHost + ":" + masterRegistryPort + "/namenode";
        nameNodeService = (INameNode) Naming.lookup(path);
    }

    //create an input stream
    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannelData = nameNodeStub.openReadonly(fileUri);
        FileNode fileNode = sdfsFileChannelData.getFileNode();
        SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(sdfsFileChannelData.getAccessToken(), sdfsFileChannelData.getFileNode().getFileSize(),
                fileNode.getBlockNum(), fileNode.getMap(), true, sdfsFileChannelData.getFileDataBlockCacheSize());
        sdfsFileChannel.setOrder(fileNode.getOrder());
        sdfsFileChannel.setBlockAmount(fileNode.getBlockNum());
        sdfsFileChannel.setFileSize(fileNode.getFileSize());
        sdfsFileChannel.setNameNodeAddress(new InetSocketAddress(NAME_NODE_IP, NAME_NODE_PORT));
        return sdfsFileChannel;
    }

    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannelData = nameNodeStub.openReadwrite(fileUri);
        FileNode fileNode = sdfsFileChannelData.getFileNode();
        SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(sdfsFileChannelData.getAccessToken(), sdfsFileChannelData.getFileNode().getFileSize(),
                fileNode.getBlockNum(), fileNode.getMap(), false, sdfsFileChannelData.getFileDataBlockCacheSize());
        sdfsFileChannel.setOrder(fileNode.getOrder());
        sdfsFileChannel.setBlockAmount(fileNode.getBlockNum());
        sdfsFileChannel.setFileSize(fileNode.getFileSize());
        sdfsFileChannel.setNameNodeAddress(new InetSocketAddress(NAME_NODE_IP, NAME_NODE_PORT));
        return sdfsFileChannel;
    }
    //create an sdfs file channel stream
    public SDFSFileChannel create(String fileUri) throws IOException {
        SDFSFileChannelData sdfsFileChannelData = nameNodeStub.create(fileUri);
        FileNode fileNode = sdfsFileChannelData.getFileNode();
        SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(sdfsFileChannelData.getAccessToken(), sdfsFileChannelData.getFileNode().getFileSize(),
                fileNode.getBlockNum(), fileNode.getMap(), false, sdfsFileChannelData.getFileDataBlockCacheSize());
        sdfsFileChannel.setOrder(fileNode.getOrder());
        sdfsFileChannel.setBlockAmount(fileNode.getBlockNum());
        sdfsFileChannel.setFileSize(fileNode.getFileSize());
        sdfsFileChannel.setNameNodeAddress(new InetSocketAddress(NAME_NODE_IP, NAME_NODE_PORT));
        return sdfsFileChannel;
    }
    //write bytes in buffer line in the blocks
    public ArrayList<LocatedBlock> writeBlock(ArrayList<LocatedBlock> blocks, ByteBuffer bufferLine) throws IOException, NotBoundException {
        int position = 0;
        byte[] whole = new byte[bufferLine.remaining()];
        bufferLine.get(whole, 0, whole.length);
        for (LocatedBlock block : blocks) {
            String path = "rmi://" + block.inetAddress.getHostAddress() + ":" + block.port + "/" + block.serviceName;
            IDataNode service = (IDataNode) Naming.lookup(path);
            int size = Math.min(Constants.DEFAULT_BLOCK_SIZE, whole.length - position);
            byte[] b = new byte[size];
            System.arraycopy(whole, position, b, 0, size);
            //int offset = service.getOffset(block);
            service.write(block.blockNumber, 0, size, b);
            block.setSize(size);
            position = position + size;
        }
        //nameNodeService.saveMetaData();
        return blocks;
    }
    //if the block is limited and then create more free blocks
    public ArrayList<LocatedBlock> createBlocks(String fileUri, int size) throws IOException {
        ArrayList<LocatedBlock> blocks = new ArrayList<>();
        ArrayList<LocatedBlock> exist = nameNodeService.getBlockLocations(fileUri);
        if (exist != null && size > 0) {
            int blockNumber = size / Constants.DEFAULT_BLOCK_SIZE ;
            if (blockNumber * Constants.DEFAULT_BLOCK_SIZE < size)
                blockNumber++;
            int blockSize = Math.min(blockNumber, exist.size());
            //System.out.println(size + " " + blockNumber + " " + blockSize);
            for (int i = 0; i < blockSize; i++)
                blocks.add(exist.get(i));
            if (blockNumber > exist.size()) {
                for (int i = 0; i < blockNumber - exist.size(); i++)
                    blocks.add(nameNodeService.addBlock(fileUri));
            }
            return blocks;
        }
        return null;
    }
    //update all blocks into disk
    public void updateBlock(String fileUri, ArrayList<LocatedBlock> blocks) throws IOException {
        nameNodeService.updateBlock(fileUri, blocks);
        nameNodeService.saveMetaData();
    }
    //read byte in the block
    public int ReadBlockByte(LocatedBlock block, long start, long end, byte b[]) throws IOException, NotBoundException {
        int size = (int) (end - start + 1);
        String hostAddress = block.inetAddress.getHostAddress();
        int port = block.port;
        String path = "rmi://" + hostAddress + ":" + port + "/datanode";
        IDataNode dataNodeService = (IDataNode) Naming.lookup(path);
        //nameNodeService.saveMetaData();
        byte[] read = new byte[size];
        //System.out.println(block.blockNumber + "," + start + "," + size);
        read = dataNodeService.read(block.blockNumber, 0, size, read);
        System.arraycopy(read, 0, b, 0, read.length);
//        for (int i = 0; i < read.length; i++)
//            System.out.println(read[i]);
        return read.length;
    }
    //create a new directory
    public void mkdir(String fileUri) throws IOException {
        nameNodeStub.mkdir(fileUri);
    }
    //remove a file
    public void rm(String fileUri) throws FileNotFoundException {
        nameNodeStub.rm(fileUri);
    }

    //rename a file
    public void rename(String fileUri, String newName) throws FileNotFoundException {
        nameNodeStub.rename(fileUri, newName);
    }
    //write the task into sdfs
    public void writeTask(String reducedFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(reducedFile));
        StringBuffer sb = new StringBuffer();
        while(true){
            String line = reader.readLine();
            if (line != null)
                sb.append(line + "\n");
            else
                break;
        }
        reader.close();
    }

    public void setNameNodeStub(NameNodeStub nameNodeStub) {
        this.nameNodeStub = nameNodeStub;
    }
}
