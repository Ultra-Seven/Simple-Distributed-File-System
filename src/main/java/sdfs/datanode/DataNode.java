/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import sdfs.Constants;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.namenode.AccessTokenPermission;
import sdfs.namenode.LocatedBlock;
import sdfs.protocol.IDataNodeProtocol;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class DataNode implements IDataNodeProtocol {
    private static final String prefix  = ".block";
    public static final int BLOCK_SIZE = 64 * 1024;
    public static final int DATA_NODE_PORT = 4349;
    private Exception exception;
    //    put off due to its difficulties
    private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new ConcurrentHashMap<>();
    private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new ConcurrentHashMap<>();
    private final Map<UUID, Map<Integer, Integer>> newCopyOnWriteMap = new ConcurrentHashMap<>();
    private final Set<Integer> garbage = new ConcurrentSkipListSet<>();
    NameNodeDataNodeStub nameNodeDataNodeStub = new NameNodeDataNodeStub(new InetSocketAddress(Constants.DEFAULT_IP, Constants.DEFAULT_PORT));
    private InetAddress inetAddress;

    public DataNode()  {
        try {
            inetAddress = InetAddress.getByName(Constants.DEFAULT_IP);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalArgumentException, RemoteException {
        exception = null;
        boolean isWritable = false;
        if (uuidReadonlyPermissionCache.get(fileAccessToken) == null && uuidReadwritePermissionCache.get(fileAccessToken) == null) {
            AccessTokenPermission accessTokenPermission = nameNodeDataNodeStub.getAccessTokenOriginalPermission(fileAccessToken, inetAddress);
            if (accessTokenPermission != null) {
                if (accessTokenPermission.isWriteable()) {
                    Set<Integer> set = (accessTokenPermission.getAllowBlocks() == null) ? new ConcurrentSkipListSet<>() : accessTokenPermission.getAllowBlocks();
                    isWritable = true;
                    if (accessTokenPermission.getAllowBlocks() != null && accessTokenPermission.getAllowBlocks().size() > set.size()) {
                        accessTokenPermission.getAllowBlocks().forEach(set::add);
                    }
                    uuidReadwritePermissionCache.putIfAbsent(fileAccessToken, set);

                } else {
                    Set<Integer> set = (accessTokenPermission.getAllowBlocks() == null) ? new ConcurrentSkipListSet<>() : accessTokenPermission.getAllowBlocks();
                    if (accessTokenPermission.getAllowBlocks() != null && accessTokenPermission.getAllowBlocks().size() > set.size()) {
                        accessTokenPermission.getAllowBlocks().forEach(set::add);
                    }
                    uuidReadonlyPermissionCache.putIfAbsent(fileAccessToken, set);
                }
            }
            else {
                exception = new IllegalAccessTokenException();
                throw (IllegalAccessTokenException)exception;
            }
        }
        if (uuidReadwritePermissionCache.get(fileAccessToken) != null && newCopyOnWriteMap.get(fileAccessToken) != null) {
            blockNumber = newCopyOnWriteMap.get(fileAccessToken).getOrDefault(blockNumber, blockNumber);
        }
        if (!isIllegal(fileAccessToken, blockNumber, isWritable)) {
            IllegalAccessTokenException illegalAccessTokenException = new IllegalAccessTokenException();
            exception = illegalAccessTokenException;
            throw illegalAccessTokenException;
        }

        File file = new File(getFilePath(blockNumber));
        byte[] read = new byte[size];
        if(file.exists()){
            if (position < 0 || position + size > file.length()) {
                exception = new IllegalArgumentException("out of bound");
                throw (IllegalArgumentException)exception;
            }
            FileInputStream fileInputStream;
            try {
                fileInputStream = new FileInputStream(file);
                fileInputStream.skip(position);
                int len = fileInputStream.read(read, 0, size);
                if(len > 0){
                    fileInputStream.close();
                    return read;
                }
                fileInputStream.close();
            } catch (FileNotFoundException e) {
                exception = new IllegalAccessTokenException();
                throw (IllegalAccessTokenException)exception;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            exception = new IllegalAccessTokenException();
            throw (IllegalAccessTokenException)exception;
        }
        return null;
    }

    private String getFilePath(int blockNumber) {
        /*
      The block size may be changed during test.
      So please use this constant.
     */
        String dirName = System.getProperty("sdfs.datanode.dir") == null ? "data"  : System.getProperty("sdfs.datanode.dir");
        return dirName + System.getProperty("file.separator") + blockNumber + prefix;
    }

    @Override
    public void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalArgumentException, RemoteException {
        exception = null;
        if (uuidReadonlyPermissionCache.get(fileAccessToken) == null && uuidReadwritePermissionCache.get(fileAccessToken) == null) {
            AccessTokenPermission accessTokenPermission = nameNodeDataNodeStub.getAccessTokenOriginalPermission(fileAccessToken, inetAddress);
            if (accessTokenPermission != null && accessTokenPermission.isWriteable()) {
                Set<Integer> set = (accessTokenPermission.getAllowBlocks() == null) ? new ConcurrentSkipListSet<>() : accessTokenPermission.getAllowBlocks();
                uuidReadwritePermissionCache.putIfAbsent(fileAccessToken, set);
                if (accessTokenPermission.getAllowBlocks() != null && accessTokenPermission.getAllowBlocks().size() > set.size()) {
                    accessTokenPermission.getAllowBlocks().forEach(set::add);
                }
                newCopyOnWriteMap.putIfAbsent(fileAccessToken, new ConcurrentHashMap<>());
//                    Map<Integer, Integer> map = nameNodeDataNodeStub.getAccessTokenNewBlocks(fileAccessToken);
//                    if (map != null) {
//                        newCopyOnWriteMap.put(fileAccessToken, map);
//                    }

                //initNewBlock(accessTokenPermission);
            }
            else {
                exception = new IllegalAccessTokenException();
                throw (IllegalAccessTokenException)exception;
            }
        }

        if (uuidReadwritePermissionCache.get(fileAccessToken) != null && newCopyOnWriteMap.get(fileAccessToken) != null) {
            int temp = newCopyOnWriteMap.get(fileAccessToken).getOrDefault(blockNumber, blockNumber);
            if (temp != blockNumber) {
                garbage.add(blockNumber);
                blockNumber = temp;
            }

        }
        if (!isIllegal(fileAccessToken, blockNumber, true)) {
            IllegalAccessTokenException illegalAccessTokenException = new IllegalAccessTokenException();
            exception = illegalAccessTokenException;
            throw illegalAccessTokenException;
        }
        if (position < 0 || position + buffer.length > Constants.DEFAULT_BLOCK_SIZE) {
            exception = new IllegalArgumentException("out of bound");
            throw (IllegalArgumentException)exception;
        }
        String path = getFilePath(blockNumber);
        File file = new File(path);
        if (newCopyOnWriteMap.get(fileAccessToken).get(blockNumber) == null && file.exists()) {
            int index = getIndex(blockNumber, uuidReadwritePermissionCache.get(fileAccessToken));
            if (index > 0) {
                LocatedBlock block = nameNodeDataNodeStub.newCopyOnWriteBlock(fileAccessToken, index);
                uuidReadwritePermissionCache.get(fileAccessToken).add(block.blockNumber);
                newCopyOnWriteMap.putIfAbsent(fileAccessToken, new ConcurrentHashMap<>());
                newCopyOnWriteMap.get(fileAccessToken).put(blockNumber, block.blockNumber);
                //System.out.println("token:" + fileAccessToken + ";original:" + blockNumber + ";new number:" + block.blockNumber);
                blockNumber = block.blockNumber;
                path = getFilePath(blockNumber);
                file = new File(path);
            }
        }

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        RandomAccessFile os;
        try {
            os = new RandomAccessFile(file, "rw");
            os.seek(position);
            os.write(buffer, 0, buffer.length);
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(int blockNumber) throws IOException {
        String path = getFilePath(blockNumber);
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        else {
            exception = new FileNotFoundException("file is not existed");
            throw (FileNotFoundException)exception;
        }
    }

    @Override
    public Exception getException() {
        return this.exception;
    }

    private boolean isIllegal(UUID fileAccessToken, int blockNumber, boolean write) {
        Set<Integer> map = write ? uuidReadwritePermissionCache.get(fileAccessToken) : uuidReadonlyPermissionCache.get(fileAccessToken);
        if (map != null) {
            if (map.contains(blockNumber)) {
                return !(newCopyOnWriteMap.get(fileAccessToken) != null && newCopyOnWriteMap.get(fileAccessToken).containsKey(blockNumber));
            }
            else {
                return newCopyOnWriteMap.get(fileAccessToken) != null && newCopyOnWriteMap.get(fileAccessToken).containsValue(blockNumber);
            }
        }
        return true;
    }

    private void initNewBlock(AccessTokenPermission accessTokenPermission) {
        //init new block
        accessTokenPermission.getAllowBlocks().forEach(block-> {
            File file = new File(getFilePath(block));
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
    private int getIndex(int blockNumber, Set<Integer> set) {
        return set.stream().filter(integer -> set.contains(blockNumber)).findFirst().orElse(-1);
    }

    Set<Integer> getGarbage() {
        return garbage;
    }
}
