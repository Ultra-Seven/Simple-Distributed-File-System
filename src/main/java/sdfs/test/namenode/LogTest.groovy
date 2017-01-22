package sdfs.namenode

import sdfs.Constants
import sdfs.exception.SDFSFileAlreadyExistException
import sdfs.protocol.INameNodeProtocol
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Paths

import static sdfs.Util.generateFilename
import static sdfs.Util.generatePort

class LogTest extends Specification {
    def dir1 = File.createTempDir().absolutePath
    def dir2 = File.createTempDir().absolutePath
    def dir3 = File.createTempDir().absolutePath
    def dir4 = File.createTempDir().absolutePath

    def "Log"() {
        System.setProperty("sdfs.namenode.dir", dir1);
        INameNodeProtocol nameNodeServer = new NameNode(Constants.DEFAULT_PORT, 20, 20, Constants.DEFAULT_IP)
        sleep(3000)
        Files.copy(Paths.get(dir1, "root.node"), Paths.get(dir2, "root.node"))
        def parentDir = generateFilename()
        nameNodeServer.mkdir(parentDir)
        for (int i = 0; i < 255; i++)
            nameNodeServer.mkdir(parentDir += "/" + generateFilename())
        def dirName = generateFilename()
        def filename = generateFilename()
        def filename2 = generateFilename()
        nameNodeServer.mkdir("$parentDir/$dirName")
        def accessToken = nameNodeServer.create("$parentDir/$filename").accessToken
        def locatedBlock = nameNodeServer.addBlocks(accessToken, 1)[0]
        def accessToken2 = nameNodeServer.create("$parentDir/$filename2").accessToken
        def locatedBlock2 = nameNodeServer.addBlocks(accessToken2, 3)[0]
        nameNodeServer.removeLastBlocks(accessToken2, 1)
        def copyOnWriteBlock2 = nameNodeServer.newCopyOnWriteBlock(accessToken2, 1)
        sleep(3000)
        nameNodeServer.closeReadwriteFile(accessToken, 1)
        nameNodeServer.closeReadwriteFile(accessToken2, Constants.DEFAULT_BLOCK_SIZE * 2)
        Files.copy(Paths.get(dir1, "root.node"), Paths.get(dir3, "root.node"))
        Files.copy(Paths.get(dir1, "namenode.log"), Paths.get(dir3, "namenode.log"))
        sleep(10000)
        Files.copy(Paths.get(dir1, "root.node"), Paths.get(dir4, "root.node"))
        System.setProperty("sdfs.namenode.dir", dir2);
        def nameNodeServer2 = new NameNode(generatePort(), 20, 20, Constants.DEFAULT_IP)
        System.setProperty("sdfs.namenode.dir", dir3);
        def nameNodeServer3 = new NameNode(generatePort(), 20, 20, Constants.DEFAULT_IP)
        System.setProperty("sdfs.namenode.dir", dir4);
        def nameNodeServer4 = new NameNode(generatePort(), 20, 20, Constants.DEFAULT_IP)

        when:
        nameNodeServer2.mkdir("$parentDir/$dirName")

        then:
        thrown(FileNotFoundException)

        when:
        nameNodeServer2.create("$parentDir/$filename")

        then:
        thrown(FileNotFoundException)

        when:
        nameNodeServer3.mkdir("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        nameNodeServer3.create("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        def fileNode = nameNodeServer3.openReadonly("$parentDir/$filename").fileNode

        then:
        fileNode.fileSize == 1
        fileNode.getBlockInfos().get(0).getLocatedBlocks().get(0) == locatedBlock

        when:
        def fileNode2 = nameNodeServer3.openReadonly("$parentDir/$filename2").fileNode

        then:
        fileNode2.fileSize == Constants.DEFAULT_BLOCK_SIZE * 2
        fileNode2.getBlockInfos().get(0).getLocatedBlocks().get(0) == locatedBlock2
        fileNode2.getBlockInfos().get(1).getLocatedBlocks().get(0) == copyOnWriteBlock2

        when:
        nameNodeServer4.mkdir("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        nameNodeServer4.create("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        def fileNode3 = nameNodeServer4.openReadonly("$parentDir/$filename").fileNode

        then:
        fileNode3.fileSize == 1
        fileNode3.getBlockInfos().get(0).getLocatedBlocks().get(0) == locatedBlock

        when:
        def fileNode4 = nameNodeServer4.openReadonly("$parentDir/$filename2").fileNode

        then:
        fileNode4.fileSize == Constants.DEFAULT_BLOCK_SIZE * 2
        fileNode4.getBlockInfos().get(0).getLocatedBlocks().get(0) == locatedBlock2
        fileNode4.getBlockInfos().get(1).getLocatedBlocks().get(0) == copyOnWriteBlock2
        //I change the 4 to zero because I write jason to file without header(4byte)
        new File(dir4, "namenode.log").size() == 0
    }
}

