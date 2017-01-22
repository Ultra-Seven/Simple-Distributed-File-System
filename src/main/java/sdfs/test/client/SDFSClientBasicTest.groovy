package sdfs.client

import sdfs.Constants
import sdfs.datanode.SDFSSlave
import sdfs.exception.SDFSFileAlreadyExistException
import sdfs.namenode.SDFSMaster
import sdfs.protocol.IDataNodeProtocol
import sdfs.protocol.INameNodeProtocol
import spock.lang.Shared
import spock.lang.Specification
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.rmi.registry.Registry

import static sdfs.Util.generateFilename

class SDFSClientBasicTest extends Specification {
    @Shared
    Registry registry
    @Shared
    INameNodeProtocol nameNodeServer
    @Shared
    IDataNodeProtocol dataNodeServer

    @Shared
    SimpleDistributedFileSystem client;


    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath);
        System.setProperty("sdfs.datanode.dir", File.createTempDir().absolutePath);
        SDFSSlave slave = new SDFSSlave()
        SDFSMaster master = new SDFSMaster()
        slave.main();
        master.main();
        client = new SimpleDistributedFileSystem(new InetSocketAddress(Constants.DEFAULT_IP, Constants.DEFAULT_PORT), 16);
    }

    def cleanupSpec() {

    }

    def "Test file tree"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        //255
        for (int i = 0; i < 255; i++)
            client.mkdir(parentDir += "/" + generateFilename())
        def dirName = generateFilename()
        def filename = generateFilename()
        client.mkdir("$parentDir/$dirName")
        client.create("$parentDir/$filename").close()

        when:
        client.mkdir("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.mkdir("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.create("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.create("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistException)

        when:
        client.openReadonly("$parentDir/${generateFilename()}")

        then:
        thrown(FileNotFoundException)

        when:
        client.openReadWrite("$parentDir/${generateFilename()}")

        then:
        thrown(FileNotFoundException)

        when:
        client.openReadonly("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)

        when:
        client.openReadWrite("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)

        when:
        client.create("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)
    }

    def "Test create empty"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        def filename = parentDir + "/" + generateFilename()

        when:
        def fc = client.create(filename)

        then:
        fc.size() == 0
        //FIXME:fc.fileNode.blockAmount
        fc.getBlockAmount() == 0
        fc.isOpen()
        fc.position() == 0
        fc.read(ByteBuffer.allocate(1)) == 0

        when:
        fc.position(-1)

        then:
        thrown(IllegalArgumentException)

        when:
        fc.position(1)

        then:
        fc.size() == 0
        fc.getBlockAmount() == 0
        fc.isOpen()
        fc.position() == 1
        fc.read(ByteBuffer.allocate(1)) == 0

        when:
        fc.close()

        then:
        !fc.isOpen()

        when:
        fc.position()

        then:
        thrown(ClosedChannelException)

        when:
        fc.position(0)

        then:
        thrown(ClosedChannelException)

        when:
        fc.read(ByteBuffer.allocate(1))

        then:
        thrown(ClosedChannelException)

        when:
        fc.write(ByteBuffer.allocate(1))

        then:
        thrown(ClosedChannelException)

        when:
        fc.size()

        then:
        thrown(ClosedChannelException)

        when:
        fc.flush()

        then:
        thrown(ClosedChannelException)

        when:
        fc.truncate(0)

        then:
        thrown(ClosedChannelException)

        when:
        fc.close()

        then:
        noExceptionThrown()

        when:
        fc = client.openReadonly(filename)

        then:
        fc.size() == 0
        fc.getBlockAmount() == 0
        fc.isOpen()
        fc.position() == 0

        when:
        fc.write(ByteBuffer.allocate(1))

        then:
        thrown(NonWritableChannelException)

        when:
        fc.truncate(0)

        then:
        thrown(NonWritableChannelException)

        when:
        fc.close()

        then:
        !fc.isOpen()

        when:
        fc.close()

        then:
        noExceptionThrown()
    }
}