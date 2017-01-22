package sdfs.datanode

import sdfs.Constants
import sdfs.client.NameNodeStub
import sdfs.exception.IllegalAccessTokenException
import sdfs.namenode.SDFSMaster
import sdfs.protocol.IDataNodeProtocol
import sdfs.protocol.INameNodeProtocol
import spock.lang.Shared
import spock.lang.Specification

import java.rmi.registry.Registry
import static sdfs.Util.generateFilename
import static sdfs.namenode.NameNode.NAME_NODE_IP
import static sdfs.namenode.NameNode.NAME_NODE_PORT

class DataNodeServerTest extends Specification {
    public static final int POSITION = Constants.DEFAULT_BLOCK_SIZE >> 2
    public static final int BUFFER_SIZE = Constants.DEFAULT_BLOCK_SIZE >> 1
    @Shared
    Registry registry
    @Shared
    INameNodeProtocol nameNodeServer
    @Shared
    IDataNodeProtocol dataNodeServer
    @Shared
    def dataBuffer = new byte[BUFFER_SIZE]
    def parentDir = generateFilename()
    def filename = parentDir + "/" + generateFilename()
    int blockNumber

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath);
        System.setProperty("sdfs.datanode.dir", File.createTempDir().absolutePath);
        SDFSMaster master = new SDFSMaster()
        master.main();

        nameNodeServer = new NameNodeStub(new InetSocketAddress(NAME_NODE_IP, NAME_NODE_PORT), 20);
        dataNodeServer = new DataNode();
        for (int i = 0; i < BUFFER_SIZE; i++)
            dataBuffer[i] = i
    }

    def cleanupSpec() {
    }

    def setup() {
        nameNodeServer.mkdir(parentDir)
    }

    private def writeData() {
        def accessToken = nameNodeServer.create(filename).accessToken
        blockNumber = nameNodeServer.addBlocks(accessToken, 1).get(0).getBlockNumber()
        dataNodeServer.write(accessToken, blockNumber, POSITION, dataBuffer)
        nameNodeServer.closeReadwriteFile(accessToken, 1)
    }

    def "Read"() {
        writeData()
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken
        def accessToken = nameNodeServer.openReadwrite(filename).accessToken

        def tempAccessToken = nameNodeServer.create(generateFilename()).accessToken
        def tempBlockNumber = nameNodeServer.addBlocks(tempAccessToken, 1)[0].getBlockNumber()
        println(tempBlockNumber)

        when:
        def b = dataNodeServer.read(tempAccessToken, tempBlockNumber, 0, Constants.DEFAULT_BLOCK_SIZE)

        then:
        //FIXME:
        thrown(IllegalAccessTokenException)
        //b.size() == Constants.DEFAULT_BLOCK_SIZE
        //b.every({ it == 0.byteValue() })

        when:
        dataNodeServer.read(UUID.randomUUID(), blockNumber, 0, 1)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken, blockNumber + 1, 0, 1)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken, -1, 0, 1)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken, blockNumber, Constants.DEFAULT_BLOCK_SIZE, 1)

        then:
        thrown(IllegalArgumentException)

        when:
        dataNodeServer.read(readonlyAccessToken, blockNumber, 0, Constants.DEFAULT_BLOCK_SIZE + 1)

        then:
        thrown(IllegalArgumentException)
        dataNodeServer.read(readonlyAccessToken, blockNumber, POSITION, BUFFER_SIZE) == dataBuffer
        dataNodeServer.read(accessToken, blockNumber, POSITION, BUFFER_SIZE) == dataBuffer
    }

    def "Write"() {
        def accessToken = nameNodeServer.create(filename).accessToken
        def blockNumber = nameNodeServer.addBlocks(accessToken, 1)[0].getBlockNumber()
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken

        when:
        dataNodeServer.write(UUID.randomUUID(), blockNumber, 0, new byte[1])

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.write(accessToken, blockNumber + 1, 0, new byte[1])

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.write(accessToken, -1, 0, new byte[1])

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.write(accessToken, blockNumber, Constants.DEFAULT_BLOCK_SIZE, new byte[1])

        then:
        thrown(IllegalArgumentException)

        when:
        dataNodeServer.write(accessToken, blockNumber, 0, new byte[Constants.DEFAULT_BLOCK_SIZE + 1])

        then:
        thrown(IllegalArgumentException)

        when:
        dataNodeServer.write(accessToken, blockNumber, POSITION, dataBuffer)

        then:
        noExceptionThrown()

        when:
        dataNodeServer.write(readonlyAccessToken, blockNumber, POSITION, dataBuffer)

        then:
        thrown(IllegalAccessTokenException)
    }

    /*def "Client level copy on write"() {
        def accessToken = nameNodeServer.create(filename).accessToken
        def blockNumber = nameNodeServer.addBlocks(accessToken, 1)[0].getBlockNumber()
        def readonlyAccessToken = nameNodeServer.openReadonly(filename).accessToken
        nameNodeServer.closeReadwriteFile(accessToken, 1)
        accessToken = nameNodeServer.openReadwrite(filename).accessToken
        def readonlyAccessToken2 = nameNodeServer.openReadonly(filename).accessToken
        def copyOnWriteBlock = nameNodeServer.newCopyOnWriteBlock(accessToken, 0).getBlockNumber()

        when:
        dataNodeServer.write(accessToken, blockNumber, POSITION, dataBuffer)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.write(accessToken, copyOnWriteBlock, POSITION, dataBuffer)

        then:
        noExceptionThrown()

        when:
        dataNodeServer.read(readonlyAccessToken2, blockNumber, POSITION, BUFFER_SIZE)

        then:
        noExceptionThrown()

        when:
        dataNodeServer.read(readonlyAccessToken2, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken, blockNumber, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)

        when:
        nameNodeServer.closeReadwriteFile(accessToken, 1)
        dataNodeServer.read(readonlyAccessToken2, blockNumber, POSITION, BUFFER_SIZE)

        then:
        noExceptionThrown()

        when:
        dataNodeServer.read(readonlyAccessToken2, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken, blockNumber, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)

        when:
        def readonlyAccessToken3 = nameNodeServer.openReadonly(filename).accessToken
        dataNodeServer.read(readonlyAccessToken3, blockNumber, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)

        when:
        dataNodeServer.read(readonlyAccessToken3, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        noExceptionThrown()
    }*/
}