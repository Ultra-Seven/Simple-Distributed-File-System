package sdfs.namenode;

import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.DirNode;
import sdfs.filetree.FileNode;
import sdfs.namenode.log.CheckPoint;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import static sdfs.namenode.NameNodeMetaData.*;

/**
 * Created by lenovo on 2016/10/1.
 */
public class NameNodeService implements INameNode {
    NameNodeMetaData nameNode;
    public NameNodeService() {
        nameNode = NameNodeMetaData.getNameNode();
    }
    public NameNodeService(NameNodeMetaData nameNode) {
        this.nameNode = nameNode;
    }

    @Override
    public FileNode open(String fileUri) {
        String[] directories = fileUri.split("/");
        DirNode node = getParentDir(fileUri);
        //TODO:file url:/xxx/xxx/xxx/ many cases
        if (node != null) {
            int id = node.findFile(directories[directories.length - 1]);
            FileNode fileNode = nameNode.getFileTable().get(id);
            if (fileNode != null) {
                FileNode newFileNode = new FileNode();
                newFileNode.setAll(fileNode);
                return newFileNode;
            }
            else
                return null;
        }
        return null;
    }

    @Override
    public FileNode create(String fileUri) throws FileNotFoundException {
//        String[] directories = fileUri.split("/");
//        if (directories.length > 1) {
//            FileNode fileNode;
//            DirNode node = getParentDir(fileUri);
//            if (node != null) {
//                int id = node.findFile(directories[directories.length - 1]);
//                fileNode = nameNode.getFileTable().get(id);
//                DirNode dirNode = nameNode.getDirTable().get(node.findDir(directories[directories.length - 1]));
//                if (fileNode == null && dirNode == null) {
//                    FileNode fileNode2 = node.createFile(directories[directories.length - 1]);
//                    FileNode newFileNode = new FileNode();
//                    newFileNode.setAll(fileNode2);
//                    return newFileNode;
//                }
//            }
//            else
//                throw new FileNotFoundException("directories can not found");
//        }
//        else {
//            int id = nameNode.getRoot().findFile(directories[directories.length - 1]);
//            FileNode fileNode = nameNode.getFileTable().get(id);
//            DirNode dirNode = nameNode.getDirTable().get(nameNode.getRoot().findDir(directories[directories.length - 1]));
//            if (fileNode == null && dirNode == null) {
//                FileNode fileNode1 = nameNode.getRoot().createFile(directories[directories.length - 1]);
//                FileNode newFileNode = new FileNode();
//                newFileNode.setAll(fileNode1);
//                return newFileNode;
//            }
//        }
//        return null;
        return null;
    }

    @Override
    public void close(String fileUri) throws IOException {
        String[] directories = fileUri.split("/");
        DirNode node = getParentDir(fileUri);
        int id = node.findFile(directories[directories.length - 1]);
        FileNode fileNode = nameNode.getFileTable().get(id);
         ;
    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistException {
        String[] directories = fileUri.split("/");
        DirNode node = getParentDir(fileUri);
        if(node != null) {
            int id = node.findDir(directories[directories.length - 1]);
            DirNode dirNode = nameNode.getDirTable().get(id);
            FileNode fileNode = nameNode.getFileTable().get(node.findFile(directories[directories.length - 1]));
            //System.out.println(dirNode.getId());
            if (dirNode == null && fileNode == null) {
                DirNode newNode = node.createDir(directories[directories.length - 1]);
                if (nameNode.getDirTable().putIfAbsent(newNode.getId(), newNode) != null) {
                    throw new SDFSFileAlreadyExistException();
                }
            }
            else
                throw new SDFSFileAlreadyExistException();
        }
        else
            System.out.println("No exist!");
        saveMetaData();
    }

    @Override
    public LocatedBlock addBlock(String fileUri) {
//        String[] directories = fileUri.split("/");
        DirNode node = getParentDir(fileUri);
//        int id = node.findFile(directories[directories.length - 1]);
//        LocatedBlock block;
//        FileNode fileNode = nameNode.getFileTable().get(id);
//        if (fileNode != null) {
//            try {
//                block = new LocatedBlock();
//                BlockInfo information = new BlockInfo();
//                information.addBlock(block);
//                fileNode.getBlockInfos().add(information);
//                return block;
//            } catch (UnknownHostException e) {
//                e.printStackTrace();
//            }
//        }
        return null;
    }

    public LocatedBlock setBlock(FileNode fileNode, LocatedBlock block) {
        if (fileNode != null) {
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(block);
            fileNode.addBlockInfo(blockInfo);
            return block;
        }
        return null;
    }

    @Override
    public ArrayList<LocatedBlock> getBlockLocations(String fileUri) {
//        FileNode fileNode = open(fileUri);
//        //System.out.println(fileUri);
//        if (fileNode != null) {
//            ArrayList<LocatedBlock> blocks = new ArrayList<>();
//            for (int i = 0; i < fileNode.getBlockInfos().size(); i++) {
//                BlockInfo info = fileNode.getBlockInfos().get(i);
//                //System.out.println(info.getLocatedBlocks().size());
//                int index = (int) (Math.random() * info.getLocatedBlocks().size());
//                LocatedBlock block = info.getLocatedBlocks().get(index);
//                blocks.add(block);
//                //System.out.println("index:" + index + ",block size:" + block.getSize() + ", block number:" + block.blockNumber);
//            }
//            return blocks;
//        }
           return null;
    }

    @Override
    public void saveMetaData() {
        nameNode.saveNameNode();
    }

    @Override
    public void updateBlock(String fileUri, ArrayList<LocatedBlock> locatedBlocks) {
//        FileNode fileNode = open(fileUri);
//        if (fileNode != null) {
//            List<BlockInfo> delList = new ArrayList<>();
//            fileNode.getBlockInfos().stream().forEach(e-> {
//                if(locatedBlocks.stream().filter(block -> e.getLocatedBlocks().get(0).blockNumber == block.blockNumber).findFirst().orElse(null) == null) {
//                    delList.add(e);
//                }
//            });
//            fileNode.removeBlockInfos(delList);
//            locatedBlocks.stream().forEach(e -> {
//                fileNode.getBlockInfos().stream().filter(a -> a.getLocatedBlocks().get(0).blockNumber == e.blockNumber).findFirst().orElse(null).getLocatedBlocks().forEach(block -> block.setAll(e));
//            });
//        }
    }

    @Override
    public void checkBlock(int block, String slave) {
//        for (Object o : nameNode.getFileTable().entrySet()) {
//            Map.Entry entry = (Map.Entry) o;
//            FileNode fileNode = (FileNode) entry.getValue();
//            fileNode.getBlockInfos().stream().forEach(e->{
//                LocatedBlock locatedBlock = e.getLocatedBlocks().stream().filter(block1 -> (block1.blockNumber == block)).findFirst().orElse(null);
//                if (locatedBlock == null && nameNode.getSlaveBooleanMap().get(slave) != null) {
//                    nameNode.getSlaveBooleanMap().remove(slave);
//                }
//            });
//        }
    }

    @Override
    public void rm(String fileUri) throws FileNotFoundException {
//        String[] directories = fileUri.split("/");
//        DirNode p = getParentDir(fileUri);
//        if(p != null) {
//            int id = p.findFile(directories[directories.length - 1]);
//            FileNode fileNode = open(fileUri);
//            if (fileNode != null) {
//                new NameNode().removeLastBlocks(fileNode.getFileUuid(), fileNode.getBlockNum());
//                p.remove(fileNode);
//                nameNode.getFileTable().remove(id);
//                nameNode.getFileUuidTable().remove(fileNode.getFileUuid());
//            }
//            else {
//                throw new FileNotFoundException(fileUri + " can not be found");
//            }
//        }
//        saveMetaData();
    }
    public DirNode getParentDir(String fileUri) {
        String[] directories = fileUri.split("/");
        Map<Integer, DirNode> hashMap = nameNode.getDirTable();
        DirNode node = nameNode.getRoot();
        for (int i = 0; i < directories.length - 1; i++) {
            int id = node.findPredecessorId(directories[i]);
            //System.out.println(id);
            if (hashMap.containsKey(id)) {
                node = hashMap.get(id);
            }
            else
                return null;
        }
        return node;
    }

}
