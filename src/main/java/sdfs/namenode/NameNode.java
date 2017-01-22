/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import com.fasterxml.jackson.databind.ObjectMapper;
import sdfs.Constants;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.DirNode;
import sdfs.filetree.Entry;
import sdfs.filetree.FileNode;
import sdfs.namenode.log.*;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.OverlappingFileLockException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
    public static int NAME_NODE_PORT = Constants.DEFAULT_PORT;
    public static String NAME_NODE_IP = Constants.DEFAULT_IP;
    private Exception exception;
    private final Map<UUID, FileNode> readonlyFile = new ConcurrentHashMap<>();
    private final Map<UUID, FileNode> readwriteFile = new ConcurrentHashMap<>();
    private final Map<Integer, UUID> conflict = new ConcurrentHashMap<>();
    //new blocks mapping
    private final Map<UUID, Map<Integer, Integer>> newFileBlocks = new ConcurrentHashMap<>();
    //commands queue used to serialize different commands
    private final ConcurrentLinkedQueue<Command> commands = new ConcurrentLinkedQueue<>();
    //instance of logger
    private Logger logger;
    private NameNodeMetaData nameNodeMetaData;
    private NameNodeService nameNodeService;
    private DataNodeList dataNodeList = new DataNodeList();

    private final int fileLocateBlockCacheSize;
    private int fileDataBlockCacheSize;
    public NameNode() {
        String path = System.getProperty("sdfs.namenode.dir") == null ? Constants.LOG_PATH : System.getProperty("sdfs.namenode.dir") + "/namenode.log";
        nameNodeMetaData = NameNodeMetaData.getNameNode();
        nameNodeService = new NameNodeService(nameNodeMetaData);
        try {
            logger = new Logger(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            recoveryFromLog(path);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        fileLocateBlockCacheSize = 0;
        fileDataBlockCacheSize = 16;

    }
    /**
     * @param fileLocatedBlockCacheSize Buffer size for file located block. By default, it should be 16.
     *                                  That means 16 block of data will be cache on local.
     *                                  And you should use LRU algorithm to replace it.
     *                                  It may be change during test. So don't assert it will equal to a constant.
     * @param fileDataBlockCacheSize    Buffer size for file data block. By default, it should be 16.
     *                                  That means 16 block of data will be cache on local.
     *                                  And you should use LRU algorithm to replace it.
     *                                  It may be change during test. So don't assert it will equal to a constant.
     */
    public NameNode(int nameNodePort, int fileLocatedBlockCacheSize, int fileDataBlockCacheSize, String nameNodeIP) {
        String path = System.getProperty("sdfs.namenode.dir") == null ? Constants.LOG_PATH : System.getProperty("sdfs.namenode.dir") + "/namenode.log";
        this.fileLocateBlockCacheSize = fileLocatedBlockCacheSize;
        this.fileDataBlockCacheSize = fileDataBlockCacheSize;
        NAME_NODE_PORT = nameNodePort;
        NAME_NODE_IP = nameNodeIP;
        nameNodeMetaData = new NameNodeMetaData();
        nameNodeService = new NameNodeService(nameNodeMetaData);
        try {
            logger = new Logger(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            recoveryFromLog(path);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws FileNotFoundException {
        exception = null;
        UUID uuid = UUID.randomUUID();
        FileNode fileNode = nameNodeService.open(fileUri);
        if (fileNode != null) {
            SDFSFileChannelData sdfsFileChannelData = new SDFSFileChannelData(uuid, fileNode, fileDataBlockCacheSize);
            readonlyFile.put(uuid, fileNode);
            return sdfsFileChannelData;
        }
        else {
            FileNotFoundException fileNotFoundException = new FileNotFoundException(fileUri + " can not be found");
            exception = fileNotFoundException;
            throw fileNotFoundException;
        }
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws FileNotFoundException, IllegalStateException {
        exception = null;
        UUID uuid = UUID.randomUUID();
        FileNode fileNode = nameNodeService.open(fileUri);
        commands.add(new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.BEGIN, fileUri));
        if (fileNode != null) {
            if (conflict.get(fileNode.getId()) != null) {
                OverlappingFileLockException overlappingFileLockException = new OverlappingFileLockException();
                exception = overlappingFileLockException;
                throw overlappingFileLockException;
            }
            conflict.put(fileNode.getId(), uuid);
            Command command = new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.COMMIT, fileUri);
            if (Command.isMKDIRValid(commands, command)) {
                commands.add(command);
                WriteStartLog writeStartLog = new WriteStartLog("start");
                writeStartLog.setAll(fileUri, uuid);
                dispatchLog(writeStartLog);
                readwriteFile.putIfAbsent(uuid, fileNode);
                return new SDFSFileChannelData(uuid, fileNode, fileDataBlockCacheSize);
            }
            else {
                commands.add(new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.ABORT, fileUri));
                WriteAbortLog writeAbortLog = new WriteAbortLog("abort");
                writeAbortLog.setAll(uuid);
                dispatchLog(writeAbortLog);
                OverlappingFileLockException overlappingFileLockException = new OverlappingFileLockException();
                exception = overlappingFileLockException;
                throw overlappingFileLockException;
            }
        }
        else {
            FileNotFoundException fileNotFoundException = new FileNotFoundException(fileUri + " can not be found");
            exception = fileNotFoundException;
            throw fileNotFoundException;
        }
    }
    //concurrent open read-write method allowing more than one writers to modify the same file
    public SDFSFileChannelData con_openReadwrite(String fileUri) throws FileNotFoundException, IllegalStateException {
        exception = null;
        UUID uuid = UUID.randomUUID();
        FileNode fileNode = nameNodeService.open(fileUri);
        commands.add(new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.BEGIN, fileUri));
        if (fileNode != null) {
            Command command = new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.COMMIT, fileUri);
            commands.add(command);
            WriteStartLog writeStartLog = new WriteStartLog("start");
            writeStartLog.setAll(fileUri, uuid);
            dispatchLog(writeStartLog);
            readwriteFile.putIfAbsent(uuid, fileNode);
            return new SDFSFileChannelData(uuid, fileNode, fileDataBlockCacheSize);
        }
        else {
            FileNotFoundException fileNotFoundException = new FileNotFoundException(fileUri + " can not be found");
            exception = fileNotFoundException;
            throw fileNotFoundException;
        }
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws IOException {
        exception = null;
        UUID uuid = UUID.randomUUID();
        //begin phase
        commands.add(new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.BEGIN, fileUri));
        CreateFileLog createFileLog = new CreateFileLog("create");
        createFileLog.setAll(fileUri, uuid);
        dispatchLog(createFileLog);
        FileNode fileNode;
        String[] directories = fileUri.split("/");
        String name = directories[directories.length - 1];
        DirNode node = directories.length > 1 ? nameNodeService.getParentDir(fileUri) : nameNodeMetaData.getRoot();
        if (node != null) {
            int id = node.findFile(name);
            fileNode = nameNodeMetaData.getFileTable().get(id);
            DirNode dirNode = nameNodeMetaData.getDirTable().get(node.findDir(name));
            //commit phase
            Command command = new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.COMMIT, fileUri);
            if (fileNode == null && dirNode == null && Command.isMKDIRValid(commands, command)) {
                commands.add(command);
                FileNode fileNode2 = node.createFile(name, nameNodeMetaData);
                FileNode newFileNode = new FileNode();
                newFileNode.setAll(fileNode2);
                commands.add(new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.END, fileUri));
                fileNode = newFileNode;
            }
            else {
                commands.add(new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.ABORT, fileUri));
                WriteAbortLog writeAbortLog = new WriteAbortLog("abort");
                writeAbortLog.setAll(uuid);
                dispatchLog(writeAbortLog);
                SDFSFileAlreadyExistException fileAlreadyExistsException = new SDFSFileAlreadyExistException();
                exception = fileAlreadyExistsException;
                throw fileAlreadyExistsException;
            }
        }
        else {
            commands.add(new Command(Command.CommandType.OPENREADWRITE, uuid, Command.Phase.ABORT, fileUri));
            WriteAbortLog writeAbortLog = new WriteAbortLog("abort");
            writeAbortLog.setAll(uuid);
            dispatchLog(writeAbortLog);
            FileNotFoundException fileNotFoundException = new FileNotFoundException("directories can not found");
            exception = fileNotFoundException;
            throw fileNotFoundException;
        }
        SDFSFileChannelData sdfsFileChannelData = new SDFSFileChannelData(uuid, fileNode, fileDataBlockCacheSize);
        if (conflict.get(fileNode.getId()) != null) {
            OverlappingFileLockException overlappingFileLockException = new OverlappingFileLockException();
            exception = overlappingFileLockException;
            throw overlappingFileLockException;
        }
        conflict.put(fileNode.getId(), uuid);
        readwriteFile.put(uuid, fileNode);
        return sdfsFileChannelData;
    }

    @Override
    public void closeReadonlyFile(UUID fileAccessToken) throws IllegalAccessTokenException {
        exception = null;
        FileNode fileNode = readonlyFile.get(fileAccessToken);
        if (fileNode == null) {
            IllegalAccessTokenException illegalStateException = new IllegalAccessTokenException();
            exception = illegalStateException;
            throw illegalStateException;
        }
        readonlyFile.remove(fileAccessToken);
    }
    //concurrent close read-write file,check if modify the same block
    public void con_closeReadwriteFile(UUID fileAccessToken, long newFileSize) {
        exception = null;
        FileNode fileNode = readwriteFile.get(fileAccessToken);
        if (fileNode != null) {
            int upper = fileNode.getBlockNum() * Constants.DEFAULT_BLOCK_SIZE;
            int lower = Math.max((fileNode.getBlockNum() - 1) * Constants.DEFAULT_BLOCK_SIZE, -1);
            if (newFileSize > lower && newFileSize <= upper) {
                //get all uuid whose writer has been modified the same file node
                List<UUID> uuids = new ArrayList<>();
                readwriteFile.entrySet().stream().filter(uuidFileNodeEntry -> uuidFileNodeEntry.getValue().getId() == fileNode.getId()).forEach(uuidFileNodeEntry -> {
                    if (uuidFileNodeEntry.getValue() != fileNode) {
                        uuids.add(uuidFileNodeEntry.getKey());
                    }
                });
                //check whether they modify the same block
                if (uuids.size() > 0) {
                    uuids.forEach(uuid -> {
                        Map<Integer, Integer> hashMap = newFileBlocks.get(uuid);
                        if (hashMap != null) {
                            hashMap.entrySet().forEach(integerIntegerEntry -> {
                                Map.Entry same = newFileBlocks.get(fileAccessToken).entrySet().stream().filter(integerIntegerEntry1 -> integerIntegerEntry == integerIntegerEntry1).findFirst().orElse(null);
                                // if any two writers modify the same block, then throw a overlapping exception
                                if (same != null) {
                                    readwriteFile.remove(fileAccessToken);
                                    newFileBlocks.remove(fileAccessToken);
                                    conflict.remove(fileNode.getId());
                                    WriteAbortLog writeAbortLog = new WriteAbortLog("abort");
                                    writeAbortLog.setAll(fileAccessToken);
                                    dispatchLog(writeAbortLog);
                                    OverlappingFileLockException overlappingFileLockException = new OverlappingFileLockException();
                                    exception = overlappingFileLockException;
                                    throw overlappingFileLockException;
                                }
                            });
                        }
                    });
                }
                WriteCommitLog writeCommitLog = new WriteCommitLog("commit");
                writeCommitLog.setAll(fileAccessToken, newFileSize);
                dispatchLog(writeCommitLog);
                fileNode.setFileSize((int)newFileSize);
                FileNode node = nameNodeMetaData.getFileTable().get(fileNode.getId());
                //update Map
                Map<Integer, Integer> map = newFileBlocks.get(fileAccessToken);
                if (map != null) {
                    map.entrySet().forEach(entry -> fileNode.getBlock(entry.getKey()).setBlockNumber(entry.getValue()));
                    newFileBlocks.remove(fileAccessToken);
                }
                if (node != null)
                    node.setAll(fileNode);
                else
                    nameNodeMetaData.getFileTable().put(fileNode.getId(), fileNode);
                readwriteFile.remove(fileAccessToken);
                conflict.remove(fileNode.getId());
            }
            else {
                readwriteFile.remove(fileAccessToken);
                newFileBlocks.remove(fileAccessToken);
                conflict.remove(fileNode.getId());
                WriteAbortLog writeAbortLog = new WriteAbortLog("abort");
                writeAbortLog.setAll(fileAccessToken);
                dispatchLog(writeAbortLog);
                IllegalArgumentException illegalArgumentException = new IllegalArgumentException();
                exception = illegalArgumentException;
                throw illegalArgumentException;
            }
        }
    }
    @Override
    public void closeReadwriteFile(UUID fileAccessToken, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        exception = null;
        FileNode fileNode = readwriteFile.get(fileAccessToken);
        if (fileNode != null) {
            int upper = fileNode.getBlockNum() * Constants.DEFAULT_BLOCK_SIZE;
            int lower = Math.max((fileNode.getBlockNum() - 1) * Constants.DEFAULT_BLOCK_SIZE, -1);
            if (newFileSize > lower && newFileSize <= upper) {
                WriteCommitLog writeCommitLog = new WriteCommitLog("commit");
                writeCommitLog.setAll(fileAccessToken, newFileSize);
                dispatchLog(writeCommitLog);
                fileNode.setFileSize((int)newFileSize);
                FileNode node = nameNodeMetaData.getFileTable().get(fileNode.getId());
                //update Map
                Map<Integer, Integer> map = newFileBlocks.get(fileAccessToken);
                if (map != null) {
                    map.entrySet().forEach(entry -> fileNode.getBlock(entry.getKey()).setBlockNumber(entry.getValue()));
                    newFileBlocks.remove(fileAccessToken);
                }
                if (node != null)
                    node.setAll(fileNode);
                else
                    nameNodeMetaData.getFileTable().put(fileNode.getId(), fileNode);
                readwriteFile.remove(fileAccessToken);
                conflict.remove(fileNode.getId());
            }
            else {
                readwriteFile.remove(fileAccessToken);
                newFileBlocks.remove(fileAccessToken);
                conflict.remove(fileNode.getId());
                WriteAbortLog writeAbortLog = new WriteAbortLog("abort");
                writeAbortLog.setAll(fileAccessToken);
                dispatchLog(writeAbortLog);
                IllegalArgumentException illegalArgumentException = new IllegalArgumentException(fileAccessToken + " block size is wrong!");
                exception = illegalArgumentException;
                throw illegalArgumentException;
            }
        }
        else {
            readwriteFile.remove(fileAccessToken);
            newFileBlocks.remove(fileAccessToken);
            WriteAbortLog writeAbortLog = new WriteAbortLog("abort");
            writeAbortLog.setAll(fileAccessToken);
            dispatchLog(writeAbortLog);
            IllegalAccessTokenException illegalStateException = new IllegalAccessTokenException();
            exception = illegalStateException;
            throw illegalStateException;
        }
    }


    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistException, FileNotFoundException {
        UUID uuid = UUID.randomUUID();
        commands.add(new Command(Command.CommandType.MKDIR, uuid, Command.Phase.BEGIN, fileUri));
        exception = null;
        String[] directories = fileUri.split("/");
        DirNode node = nameNodeService.getParentDir(fileUri);
        if(node != null) {
            int id = node.findDir(directories[directories.length - 1]);
            DirNode dirNode = nameNodeMetaData.getDirTable().get(id);
            FileNode fileNode = nameNodeMetaData.getFileTable().get(node.findFile(directories[directories.length - 1]));
            if (dirNode == null && fileNode == null) {
                Command command = new Command(Command.CommandType.MKDIR, uuid, Command.Phase.COMMIT, fileUri);
                if (Command.isMKDIRValid(commands, command)) {
                    commands.add(command);
                    MkdirLog mkdirLog = new MkdirLog("mkdir");
                    mkdirLog.setAll(fileUri);
                    dispatchLog(mkdirLog);
                    DirNode newNode = node.createDir(directories[directories.length - 1]);
                    nameNodeMetaData.getDirTable().putIfAbsent(newNode.getId(), newNode);
                }
                else {
                    SDFSFileAlreadyExistException sdfsFileAlreadyExistException = new SDFSFileAlreadyExistException();
                    exception = sdfsFileAlreadyExistException;
                    commands.removeAll(commands.stream().filter(command2 -> command2.getUuid() == uuid).collect(Collectors.toList()));
                    throw sdfsFileAlreadyExistException;
                }
                commands.add(new Command(Command.CommandType.MKDIR, uuid, Command.Phase.END, fileUri));
            }
            else {
                SDFSFileAlreadyExistException sdfsFileAlreadyExistException = new SDFSFileAlreadyExistException();
                exception = sdfsFileAlreadyExistException;
                commands.removeAll(commands.stream().filter(command -> command.getUuid() == uuid).collect(Collectors.toList()));
                throw sdfsFileAlreadyExistException;
            }
        }
        else {
            FileNotFoundException fileNotFoundException = new FileNotFoundException();
            exception = fileNotFoundException;
            commands.removeAll(commands.stream().filter(command -> command.getUuid() == uuid).collect(Collectors.toList()));
            throw fileNotFoundException;
        }
        commands.removeAll(commands.stream().filter(command -> command.getUuid() == uuid).collect(Collectors.toList()));
    }


    public LocatedBlock getBlock(UUID fileUuid, int blockNumber) throws IllegalStateException, IndexOutOfBoundsException {
        exception = null;
        FileNode fileNode = nameNodeMetaData.getFileUuidTable().get(fileUuid);
        if (fileNode != null) {
            if (blockNumber >= 0)
                return fileNode.getBlock(blockNumber);
            else {
                IndexOutOfBoundsException indexOutOfBoundsException = new IndexOutOfBoundsException("the block number is out of bound!");
                exception = indexOutOfBoundsException;
                throw indexOutOfBoundsException;
            }
        }
        else {
            IllegalStateException illegalStateException = new IllegalStateException("no such a file");
            exception = illegalStateException;
            throw illegalStateException;
        }
    }


    public Map<Integer, LocatedBlock> getBlocks(UUID fileUuid, int startBlockNumber, int blockAmount) throws IllegalStateException, IndexOutOfBoundsException {
        exception = null;
        Map<Integer, LocatedBlock> map = new HashMap<>();
        FileNode fileNode = nameNodeMetaData.getFileUuidTable().get(fileUuid);
        if (fileNode != null) {
            if (startBlockNumber < 0 || blockAmount < 0 || startBlockNumber + blockAmount > fileNode.getBlockNum()) {
                IndexOutOfBoundsException indexOutOfBoundsException = new IndexOutOfBoundsException("index is out of bound");
                exception = indexOutOfBoundsException;
                throw indexOutOfBoundsException;
            }
            for (int i = startBlockNumber; i < startBlockNumber + blockAmount; i++) {
                LocatedBlock block;
                if ((block = fileNode.getBlock(i)) != null) {
                    map.put(i, block);
                }
            }
            return map;
        }
        else {
            IllegalStateException illegalStateException = new IllegalStateException("no such a file");
            exception = illegalStateException;
            throw illegalStateException;
        }
    }


    public Map<Integer, LocatedBlock> getBlocks(UUID fileUuid, int[] blockNumbers) throws IllegalStateException, IndexOutOfBoundsException {
        exception = null;
        Map<Integer, LocatedBlock> map = new HashMap<>();
        FileNode fileNode = nameNodeMetaData.getFileUuidTable().get(fileUuid);
        if (fileNode != null) {
            for (int blockNumber : blockNumbers) {
                if (blockNumber < 0 || blockNumber > fileNode.getBlockNum()) {
                    IndexOutOfBoundsException indexOutOfBoundsException = new IndexOutOfBoundsException("index is out of bound");
                    exception = indexOutOfBoundsException;
                    throw indexOutOfBoundsException;
                }
                LocatedBlock block = fileNode.getBlock(blockNumber);
                map.put(blockNumber, block);
            }
            return map;
        }
        else {
            IllegalStateException illegalStateException = new IllegalStateException("no such a file");
            exception = illegalStateException;
            throw illegalStateException;
        }
    }


    private LocatedBlock addBlock(UUID fileUuid) throws IllegalAccessTokenException {
        exception = null;
        FileNode fileNode = readwriteFile.get(fileUuid);
        if (fileNode != null) {
            LocatedBlock block = null;
            try {
                block = new LocatedBlock();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(block);
            fileNode.addBlockInfo(blockInfo);
            return block;
        }
        else {
            IllegalAccessTokenException illegalAccessTokenException = new IllegalAccessTokenException();
            exception = illegalAccessTokenException;
            throw illegalAccessTokenException;
        }
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException{
        exception = null;
        if (blockAmount > 0) {
            List<LocatedBlock> blocks = new ArrayList<>();
            for (int i = 0; i < blockAmount; i++) {
                LocatedBlock block = addBlock(fileAccessToken);
                AddBlockLog addBlockLog = new AddBlockLog("addblock");
                addBlockLog.setAll(fileAccessToken, block);
                dispatchLog(addBlockLog);
                blocks.add(block);
            }
            return blocks;
        }
        else {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException("IllegalArgumentException");
            exception = illegalArgumentException;
            throw illegalArgumentException;
        }
    }

    private void removeLastBlock(UUID fileUuid) throws IllegalAccessTokenException, IndexOutOfBoundsException, RemoteException {
        exception = null;
        FileNode fileNode = readwriteFile.get(fileUuid);
        if (fileNode != null) {
            if (fileNode.getBlockNum() > 0) {
                fileNode.removeLastBlock();
            }
            else {
                IndexOutOfBoundsException illegalStateException = new IndexOutOfBoundsException("there is no block for removal!");
                exception = illegalStateException;
                throw illegalStateException;
            }
        }
        else {
            IllegalAccessTokenException illegalAccessTokenException = new IllegalAccessTokenException();
            exception = illegalAccessTokenException;
            throw illegalAccessTokenException;
        }
    }

    @Override
    public void removeLastBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException, RemoteException {
        if (blockAmount > 0) {
            RemoveBlocksLog removeBlocksLog = new RemoveBlocksLog("remove");
            removeBlocksLog.setAll(fileAccessToken, blockAmount);
            dispatchLog(removeBlocksLog);
            for (int i = 0; i < blockAmount; i++) {
                removeLastBlock(fileAccessToken);
            }
        }
        else {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException("IllegalArgumentException");
            exception = illegalArgumentException;
            throw illegalArgumentException;
        }
    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException, RemoteException {
        exception = null;
        FileNode fileNode = readwriteFile.get(fileAccessToken);
        if (fileNode != null) {
            LocatedBlock block = fileNode.getBlockInfos().get(fileBlockNumber).getBlock();
            int blockNumber = block.blockNumber;
            LocatedBlock newFileBlock = null;
            try {
                newFileBlock = new LocatedBlock();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            newFileBlock.setAll(block);
            CopyOnWriteBlockLog copyOnWriteBlockLog = new CopyOnWriteBlockLog("copyonwrite");
            copyOnWriteBlockLog.setAll(fileAccessToken, fileBlockNumber, newFileBlock);
            dispatchLog(copyOnWriteBlockLog);
            Map blocks = newFileBlocks.get(fileAccessToken);
            if (blocks == null) {
                blocks = new ConcurrentHashMap();
                newFileBlocks.put(fileAccessToken, blocks);
            }
            if (blocks.get(blockNumber) == null) {
                blocks.put(blockNumber, newFileBlock.blockNumber);
            }
            else {
                IllegalStateException illegalStateException = new IllegalStateException("there is already copy on write on this file block");
                exception = illegalStateException;
                throw illegalStateException;
            }
            return newFileBlock;
        }
        else {
            IllegalAccessTokenException illegalAccessTokenException = new IllegalAccessTokenException();
            exception = illegalAccessTokenException;
            throw illegalAccessTokenException;
        }
    }

    @Override
    public void sendHeartBeat(String ip, int port) {
        dataNodeList.addEntry(new DataNodeListEntry(ip, port, System.currentTimeMillis()));
    }

    @Override
    public void rm(String fileUri) throws FileNotFoundException {
        exception = null;
        UUID uuid = UUID.randomUUID();
        commands.add(new Command(Command.CommandType.RM, uuid, Command.Phase.BEGIN, fileUri));
        String[] directories = fileUri.split("/");
        DirNode p = nameNodeService.getParentDir(fileUri);
        if(p != null) {
            int id = p.findFile(directories[directories.length - 1]);
            FileNode fileNode = nameNodeService.open(fileUri);
            if (fileNode != null) {
                try {
                    removeLastBlocks(fileNode.getFileUuid(), fileNode.getBlockNum());
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                p.remove(fileNode);
                Command command = new Command(Command.CommandType.RM, uuid, Command.Phase.COMMIT, fileUri);
                if (Command.isDELETEValid(commands, command)) {
                    commands.add(command);
                    RMLog renameLog = new RMLog("rm");
                    renameLog.setAll(fileUri);
                    dispatchLog(renameLog);
                    nameNodeMetaData.getFileTable().remove(id);
                    nameNodeMetaData.getFileUuidTable().remove(fileNode.getFileUuid());
                }
                else {
                    commands.add(new Command(Command.CommandType.RM, uuid, Command.Phase.ABORT, fileUri));
                    return;
                }
            }
            else {
                FileNotFoundException fileNotFoundException = new FileNotFoundException(fileUri + " can not be found");
                exception = fileNotFoundException;
                throw fileNotFoundException;
            }
        }
        commands.add(new Command(Command.CommandType.RM, uuid, Command.Phase.END, fileUri));
    }

    @Override
    public void rename(String fileUri, String newName) throws FileNotFoundException {
        exception = null;
        UUID uuid = UUID.randomUUID();
        commands.add(new Command(Command.CommandType.RENAME, uuid, Command.Phase.BEGIN, fileUri));
        FileNode fileNode = nameNodeService.open(fileUri);
        DirNode dirNode = nameNodeService.getParentDir(fileUri);
        if (fileNode != null && dirNode != null) {
            Entry target = dirNode.getEntries().stream().filter(entry -> entry.getId() == fileNode.getId()).findFirst().orElse(null);
            if (target != null) {
                Command command = new Command(Command.CommandType.RENAME, uuid, Command.Phase.COMMIT, fileUri);
                if (Command.isRENAMEValid(commands, command)) {
                    commands.add(command);
                    RenameLog renameLog = new RenameLog("rename");
                    renameLog.setAll(fileUri, newName);
                    dispatchLog(renameLog);
                    target.setName(newName);
                    FileNode node = nameNodeMetaData.getFileTable().get(fileNode.getId());
                    node.setAll(fileNode);
                    commands.add(new Command(Command.CommandType.RENAME, uuid, Command.Phase.END, fileUri));
                }
                else
                    commands.add(new Command(Command.CommandType.RENAME, uuid, Command.Phase.ABORT, fileUri));
            }
            else {
                commands.add(new Command(Command.CommandType.RENAME, uuid, Command.Phase.ABORT, fileUri));
                exception = new FileNotFoundException(fileUri + " can not be found");
                throw (FileNotFoundException)exception;
            }
        }
        else {
            commands.add(new Command(Command.CommandType.RENAME, uuid, Command.Phase.ABORT, fileUri));
            exception = new FileNotFoundException(fileUri + " can not be found");
            throw (FileNotFoundException)exception;
        }
    }

    @Override
    public Exception getException() {
        return exception;
    }

    @Override
    public void setFileDataBlockCacheSize(int size) {
        fileDataBlockCacheSize = size;
    }

    @Override
    public void setBlocks(UUID fileUuid, ArrayList<LocatedBlock> blocks) throws ClosedChannelException {
        exception = null;
        FileNode fileNode = readwriteFile.get(fileUuid);
        if (fileNode != null) {
            fileNode.removeAll();
            blocks.forEach(block -> nameNodeService.setBlock(fileNode, block));
        }
        else {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            exception = closedChannelException;
            throw closedChannelException;
        }
    }

    @Override
    public AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken, InetAddress inetAddress) throws RemoteException {
        AccessTokenPermission accessTokenPermission = new AccessTokenPermission(false, null);
        FileNode fileNode;
        if ((fileNode = readwriteFile.get(fileAccessToken)) != null) {
            accessTokenPermission.setWriteable(true);
            accessTokenPermission.setAllowBlocks(getAllowableBlocks(fileNode, inetAddress));
        }
        else if ((fileNode = readonlyFile.get(fileAccessToken)) != null) {
            accessTokenPermission.setAllowBlocks(getAllowableBlocks(fileNode, inetAddress));
        }
        else {
            System.out.println("wrong input");
            return null;
        }
        return accessTokenPermission;
    }

    @Override
    public Map<Integer, Integer> getAccessTokenNewBlocks(UUID fileAccessToken, InetAddress inetAddress) throws RemoteException {
        Map<Integer, Integer> blocks = new ConcurrentHashMap<>();
        FileNode fileNode;
        if (readonlyFile.containsKey(fileAccessToken))
            fileNode = readonlyFile.get(fileAccessToken);
        else
            fileNode = readwriteFile.get(fileAccessToken);
        newFileBlocks.get(fileAccessToken).entrySet().forEach(integerIntegerEntry -> {
            LocatedBlock block2 = fileNode.getBlock(integerIntegerEntry.getKey());
            if (block2.getInetAddress().getHostAddress().equals(inetAddress.getHostAddress()))
                blocks.put(integerIntegerEntry.getKey(), integerIntegerEntry.getValue());
        });
        return blocks;
    }
    private Set<Integer> getAllowableBlocks(FileNode fileNode, InetAddress inetAddress) {
        Set<Integer> set = new ConcurrentSkipListSet<>();
        fileNode.getBlockInfos().forEach(locatedBlocks -> locatedBlocks.forEach(block -> {
            if (block.getInetAddress().getHostAddress().equals(inetAddress.getHostAddress()))
                set.add(block.getBlockNumber());
        }));
        return set;
    }
    //dispatch logs into log queue
    private void dispatchLog(Log log){
        try {
            logger.addLog(log);
            if (log instanceof WriteCommitLog || log instanceof MkdirLog || log instanceof RenameLog || log instanceof RMLog) {
                logger.flushLog();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //recover from logs in the disk
    private void recoveryFromLog(String logPath) throws IOException, ClassNotFoundException {
        System.out.println(">>>>>>>>>recovery");
        File file = new File(logPath);
        HashMap<UUID, List<Log>> hashMap = new HashMap<>();
        ArrayList<List<Log>> recoveryList = new ArrayList<>();
        if(file.exists() && file.length() > 0 ){
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            ObjectMapper mapper = new ObjectMapper();
            Log operation;
            //read logs line by line
            while ((line = reader.readLine()) != null) {
                operation = mapper.readValue(line, Log.class);
                if (operation instanceof AddBlockLog) {
                    AddBlockLog addBlockLog = (AddBlockLog) operation;
                    hashMap.putIfAbsent(addBlockLog.getFileAccessToken(), new ArrayList<>());
                    hashMap.get(addBlockLog.getFileAccessToken()).add(addBlockLog);
                } else if (operation instanceof CopyOnWriteBlockLog) {
                    CopyOnWriteBlockLog copyOnWriteBlockLog = (CopyOnWriteBlockLog) operation;
                    hashMap.putIfAbsent(copyOnWriteBlockLog.getFileAccessToken(), new ArrayList<>());
                    hashMap.get(copyOnWriteBlockLog.getFileAccessToken()).add(copyOnWriteBlockLog);
                } else if (operation instanceof CreateFileLog) {
                    CreateFileLog createFileLog = (CreateFileLog) operation;
                    hashMap.putIfAbsent(createFileLog.getFileAccessToken(), new ArrayList<>());
                    hashMap.get(createFileLog.getFileAccessToken()).add(createFileLog);
                } else if (operation instanceof MkdirLog) {
                    MkdirLog mkdirLog = (MkdirLog) operation;
                    List<Log> list = new ArrayList<>();
                    list.add(mkdirLog);
                    recoveryList.add(list);
                    //mkdirLog.redo(nameNodeService, this, nameNodeMetaData);
                } else if (operation instanceof RemoveBlocksLog) {
                    RemoveBlocksLog removeBlocksLog = (RemoveBlocksLog) operation;
                    hashMap.putIfAbsent(removeBlocksLog.getFileAccessToken(), new ArrayList<>());
                    hashMap.get(removeBlocksLog.getFileAccessToken()).add(removeBlocksLog);
                } else if (operation instanceof WriteAbortLog) {
                    WriteAbortLog writeAbortLog = (WriteAbortLog) operation;
                    List<Log> list = hashMap.get(writeAbortLog.getFileAccessToken());
                    list.add(writeAbortLog);
                    recoveryList.add(list);
                    //recovery(list);
                } else if (operation instanceof WriteCommitLog) {
                    WriteCommitLog writeCommitLog = (WriteCommitLog) operation;
                    List<Log> list = hashMap.get(writeCommitLog.getFileAccessToken());
                    list.add(writeCommitLog);
                    recoveryList.add(list);
                    //recovery(list);
                } else if (operation instanceof WriteStartLog) {
                    WriteStartLog writeStartLog = (WriteStartLog) operation;
                    hashMap.putIfAbsent(writeStartLog.getFileAccessToken(), new ArrayList<>());
                    hashMap.get(writeStartLog.getFileAccessToken()).add(writeStartLog);
                } else if (operation instanceof CheckPoint) {
                    //purgeHashMap(hashMap);
                    recoveryList = new ArrayList<>();

                } else
                    throw new IllegalStateException();
            }
            recoveryList.forEach(this::recovery);
        }
    }
    //call redo functions
    private void recovery(List<Log> logs) {
        System.out.println("recovery");
        logs.forEach(log -> {
            log.redo(nameNodeService, this, nameNodeMetaData);
        });
    }

    private void purgeHashMap(Map<UUID, List<Log>> listMap) {
        listMap.entrySet().forEach(uuidListEntry -> {
            List<Log> list = uuidListEntry.getValue();
            Log log = list.stream().filter(log1 -> log1 instanceof WriteCommitLog || log1 instanceof WriteAbortLog).findFirst().orElse(null);
            if (log != null) {
                listMap.remove(uuidListEntry.getKey());
            }
        });
    }

    public void setNameNodeMetaData(NameNodeMetaData nameNodeMetaData) {
        this.nameNodeMetaData = nameNodeMetaData;
    }

    public Map<Integer, UUID> getConflict() {
        return conflict;
    }

    public Map<UUID, FileNode> getReadonlyFile() {
        return readonlyFile;
    }

    public Map<UUID, FileNode> getReadwriteFile() {
        return readwriteFile;
    }

    public Map<UUID, Map<Integer, Integer>> getNewFileBlocks() {
        return newFileBlocks;
    }

    public NameNodeService getNameNodeService() {
        return nameNodeService;
    }
}
