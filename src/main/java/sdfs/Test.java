package sdfs;

import org.apache.commons.lang3.RandomStringUtils;
import sdfs.client.DFSClient;
import sdfs.client.SimpleDistributedFileSystem;
import sdfs.client.SDFSFileChannel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.util.Arrays;

/**
 * Created by lenovo on 2016/10/4.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        DFSClient client = new DFSClient(Constants.DEFAULT_IP, Constants.DEFAULT_PORT);
        SimpleDistributedFileSystem simpleDistributedFileSystem = new SimpleDistributedFileSystem(new InetSocketAddress(Constants.DEFAULT_IP, Constants.DEFAULT_PORT), 16);
//        SDFSFileChannel s1 = simpleDistributedFileSystem.create("1.txt");
//        SDFSFileChannel s2 = simpleDistributedFileSystem.openReadonly("1.txt");
//        byte[] b1 = {(byte) 0, (byte) 1, (byte) 2};
//        s1.write(ByteBuffer.wrap(b1));
//        System.out.println(s2.getBlockAmount());
//        s1.close();
//        System.out.println(s2.getBlockAmount());
//       writeTest("test1.txt", simpleDistributedFileSystem);
//        try {
//            readTest("test1.txt", simpleDistributedFileSystem);
//        } catch (NotBoundException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        //hybridTest1("test1.txt", simpleDistributedFileSystem);


        //rmTest("test1.txt", simpleDistributedFileSystem);

        //SimpleDistributedFileSystem simpleDistributedFileSystem = new SimpleDistributedFileSystem(client);
        //writeTest("test2", simpleDistributedFileSystem);
        //writeTest("/test1", simpleDistributedFileSystem);
        //readTest("/test2", simpleDistributedFileSystem);
        //mkdirTest("sd", simpleDistributedFileSystem);
        //rmTest("test1", simpleDistributedFileSystem);

        //rename test
        //writeTest("test1.txt", simpleDistributedFileSystem);
        //simpleDistributedFileSystem.rename("test1.txt", "test.txt");

        //test case
        //testcase1(simpleDistributedFileSystem);
        //testcase2(simpleDistributedFileSystem);
        //testcase3(simpleDistributedFileSystem);
        //testcase4(simpleDistributedFileSystem);
        testcase5(simpleDistributedFileSystem);

    }

    public static void writeTest(String fileUri, SimpleDistributedFileSystem simpleDistributedFileSystem) {
        byte[] bytes = new byte[10];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = (byte) i;
        try {
            SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.create(fileUri);
            sdfsFileChannel.write(ByteBuffer.wrap(bytes));
            sdfsFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeTest2(String fileUri, SimpleDistributedFileSystem simpleDistributedFileSystem) {
        byte[] bytes = new byte[20];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = (byte) ('a' + i);
        try {
            SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.openReadWrite(fileUri);
            sdfsFileChannel.position(10);
            sdfsFileChannel.write(ByteBuffer.wrap(bytes));
            sdfsFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readTest(String fileUri, SimpleDistributedFileSystem simpleDistributedFileSystem) throws Exception {

        byte[] b = new byte[20];
        int p = 0;
        ByteBuffer byteBuffer = ByteBuffer.wrap(b);
        SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.openReadonly(fileUri);
        p = sdfsFileChannel.read(byteBuffer);
        sdfsFileChannel.close();
        if (b != null && p >= 0) {
            System.out.println(Arrays.toString(byteBuffer.array()));
        } else
            System.out.println("wrong");
    }

    public static void mkdirTest(String fileUri, SimpleDistributedFileSystem simpleDistributedFileSystem) {
        try {
            simpleDistributedFileSystem.mkdir(fileUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void rmTest(String fileUri, SimpleDistributedFileSystem simpleDistributedFileSystem) {
        try {
            simpleDistributedFileSystem.delete(fileUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void hybridTest1(String fileUri, SimpleDistributedFileSystem simpleDistributedFileSystem) {
        try {
            SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.openReadWrite(fileUri);
            byte[] b = new byte[5];
            sdfsFileChannel.read(ByteBuffer.wrap(b));
            byte[] c = {1,2,3,4,5};
            sdfsFileChannel.write(ByteBuffer.wrap(c));
            sdfsFileChannel.position(0);
            byte[] bytes = new byte[10];
            sdfsFileChannel.read(ByteBuffer.wrap(bytes));
            System.out.println(Arrays.toString(bytes));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void testcase1(SimpleDistributedFileSystem simpleDistributedFileSystem) throws IOException {
        System.out.println("Test file tree");
        String parentDir = generateFilename();
        simpleDistributedFileSystem.mkdir(parentDir);
        for (int i = 0; i < 255; i++)
            simpleDistributedFileSystem.mkdir(parentDir + "/" + generateFilename());
        String dirName = generateFilename();
        String filename = generateFilename();
        simpleDistributedFileSystem.mkdir(parentDir+"/"+dirName);
        simpleDistributedFileSystem.create(parentDir+"/"+filename).close();

        simpleDistributedFileSystem.create(generateFilename() + "/" + filename);
        //simpleDistributedFileSystem.create(parentDir+"/"+dirName);
    }
    public static void testcase2(SimpleDistributedFileSystem simpleDistributedFileSystem) throws IOException {
        System.out.println("Test create empty");
        String parentDir = generateFilename();
        simpleDistributedFileSystem.mkdir(parentDir);
        String filename = parentDir + "/" + generateFilename();
        SDFSFileChannel fc = simpleDistributedFileSystem.create(filename);
        ByteBuffer buffer = ByteBuffer.allocate(1);
        System.out.println((fc.size()==0) + " " + (fc.getBlockAmount()==0) + " " + fc.isOpen() + " " + (fc.position()==0) + " " + (fc.read(buffer)==0));
        try {
            fc.position(-1);
        }catch (IllegalArgumentException e) {
            System.out.println("IllegalArgumentException true");
        }
        fc.position(1);
        System.out.println((fc.size()==0) + " " + (fc.getBlockAmount()==0) + " " + fc.isOpen() + " " + (fc.position()==1) + " " + (fc.read(buffer)==0));
        fc.close();
        System.out.println(!fc.isOpen());
        try {
            fc.position();
        }catch (ClosedChannelException e) {
            System.out.println("ClosedChannelException true");
        }
        try {
            fc.position(0);
        }catch (ClosedChannelException e) {
            System.out.println("ClosedChannelException true");
        }
        try {
            fc.read(buffer);
        }catch (ClosedChannelException e) {
            System.out.println("ClosedChannelException true");
        }
        try {
            fc.write(buffer);
        }catch (ClosedChannelException e) {
            System.out.println("ClosedChannelException true");
        }
        try {
            fc.size();
        }catch (ClosedChannelException e) {
            System.out.println("ClosedChannelException true");
        }
        try {
            fc.flush();
        }catch (ClosedChannelException e) {
            System.out.println("ClosedChannelException true");
        }
        try {
            fc.truncate(0);
        }catch (ClosedChannelException e) {
            System.out.println("ClosedChannelException true");
        }
        fc = simpleDistributedFileSystem.openReadonly(filename);
        System.out.println((fc.size()==0) + " " + (fc.getBlockAmount()==0) + " " + fc.isOpen() + " " + (fc.position()==0));
        try {
            fc.truncate(0);
        }catch (NonWritableChannelException e) {
            System.out.println("NonWritableChannelException true");
        }
        try {
            fc.write(buffer);
        }catch (NonWritableChannelException e) {
            System.out.println("NonWritableChannelException true");
        }
    }
    public static void testcase3(SimpleDistributedFileSystem simpleDistributedFileSystem) throws IOException {
        int FILE_SIZE = 2 * Constants.DEFAULT_BLOCK_SIZE + 2;
        ByteBuffer dataBuffer = ByteBuffer.allocate(FILE_SIZE);
        ByteBuffer buffer = ByteBuffer.allocate(FILE_SIZE);
        String parentDir = generateFilename();
        String filename = parentDir + "/" + generateFilename();
        //set up
        for (int i = 0; i < FILE_SIZE; i++)
            dataBuffer.put((byte) i);
        simpleDistributedFileSystem.mkdir(parentDir);
//        SDFSFileChannel fc = simpleDistributedFileSystem.create(filename);
//        dataBuffer.position(0);
//        fc.write(dataBuffer);
//        fc.close();

        int fileSize = FILE_SIZE;
        SDFSFileChannel fc = simpleDistributedFileSystem.create(filename);
        dataBuffer.position(0);
        System.out.println((fc.write(dataBuffer) == fileSize) + " " + (fc.size() == fileSize) + " " + fc.getBlockAmount() + " " + fc.position() + " " + (fc.write(dataBuffer) == 0));
        fc.truncate(fileSize + 1);
        System.out.println((fc.position() == fileSize) + " " + (fc.size() == fileSize));

        SDFSFileChannel fc2 = simpleDistributedFileSystem.openReadonly(filename);
        System.out.println(fc2.size() + " " + fc2.getBlockAmount() + " " + fc2.isOpen() + " " + fc2.position() + " ");

        fc.close();
        System.out.println(fc2.size() + " " + fc2.getBlockAmount() + " " + fc2.isOpen() + " " + fc2.position() + " ");
        fc = simpleDistributedFileSystem.openReadonly(filename);
        System.out.println((fc.size() == fileSize)+ " " + fc.getBlockAmount() + " " + fc.position() + " " + (fc.read(buffer) == fileSize) + " " + equalBuffer(buffer, dataBuffer));
        fc.close();
        fc2.close();
    }
    public static void testcase4(SimpleDistributedFileSystem simpleDistributedFileSystem) throws IOException {
        int FILE_SIZE = 2 * Constants.DEFAULT_BLOCK_SIZE + 2;
        ByteBuffer dataBuffer = ByteBuffer.allocate(FILE_SIZE);
        ByteBuffer buffer = ByteBuffer.allocate(FILE_SIZE);
        for (int i = 0; i < FILE_SIZE; i++)
            dataBuffer.put((byte)i);
        String parentDir = generateFilename();
        simpleDistributedFileSystem.mkdir(parentDir);
        String filename = parentDir + "/" + generateFilename();
        int fileSize = 2 * Constants.DEFAULT_BLOCK_SIZE + 2;
        int secondPosition = 3 * Constants.DEFAULT_BLOCK_SIZE - 1;
        SDFSFileChannel fc = simpleDistributedFileSystem.create(filename);
        dataBuffer.position(0);
        fc.write(dataBuffer);
        fc.close();


        fc = simpleDistributedFileSystem.openReadWrite(filename);
        buffer.position(0);
        System.out.println((fc.size() == fileSize) + " " + (fc.getBlockAmount() == getBlockAmount(fileSize)) + " " + (fc.position() == 0) + " " +
                (fc.read(buffer) == fileSize) + " " + equalBuffer(buffer, dataBuffer));


        fc.position(secondPosition);
        buffer.position(0);
        System.out.println((fc.size() == fileSize) + " " + (fc.getBlockAmount() == getBlockAmount(fileSize)) + " " + (fc.position() == secondPosition) + " " +
                (fc.read(buffer) == 0) + " " + (fc.write(dataBuffer) == 0));


        dataBuffer.position(0);
        System.out.println((fc.write(dataBuffer) == fileSize) + " " + (fc.size() == secondPosition + fileSize) + " " +
                (fc.getBlockAmount() == getBlockAmount(secondPosition + fileSize)) + " " + (fc.position() == secondPosition + fileSize) + " " + (fc.read(buffer) == 0));


        fc.position(0);
        System.out.println((fc.read(buffer) == fileSize) + " " + (fc.position() == fileSize) + " " + (fc.size() == secondPosition + fileSize) + " " +
                equalBuffer(buffer, dataBuffer) + " " + (fc.read(buffer) == 0) + " " + (fc.size() == secondPosition + fileSize) + " " +
                (fc.getBlockAmount() == getBlockAmount(secondPosition + fileSize)) + " " + (fc.position() == fileSize));


        fc.truncate(secondPosition + fileSize + 1);
        System.out.println((fc.size() == secondPosition + fileSize) + " " + (fc.getBlockAmount() == getBlockAmount(secondPosition + fileSize))
                + " " + (fc.position() == fileSize));


        buffer.position(0);
        System.out.println((fc.read(buffer) == fileSize));

        buffer.position(0);
        for (int i = 0; i < Constants.DEFAULT_BLOCK_SIZE - 3; i++) {
            byte b;
            if ((b = buffer.get()) != (byte) 0) {
                System.out.println("wrong " + b + " " + (byte)0);
                break;
            }
            if (i == Constants.DEFAULT_BLOCK_SIZE - 4)
                System.out.println("loop test is right!");
        }
        System.out.println(buffer.get() == 0);
        System.out.println(buffer.get() == 1);
        System.out.println(buffer.get() == 2);
        System.out.println(buffer.get() == 3);
        System.out.println(buffer.get() == 4);

        fc.truncate(fileSize);
        System.out.println((fc.size() == fileSize) + " " + (fc.getBlockAmount() == getBlockAmount(fileSize))
                + " " + (fc.position() == fileSize));


        fc.close();
        fc = simpleDistributedFileSystem.openReadWrite(filename);
        buffer.position(0);
        System.out.println((fc.size() == fileSize) + " " + (fc.getBlockAmount() == getBlockAmount(fileSize)) + " " +
                (fc.position() == 0) + " " + (fc.read(buffer) == fileSize) + " " + equalBuffer(buffer, dataBuffer));

        fc.close();

    }

    public static void testcase5(SimpleDistributedFileSystem simpleDistributedFileSystem) throws IOException {
        int FILE_SIZE = 2 * Constants.DEFAULT_BLOCK_SIZE + 2;
        int fileSize = FILE_SIZE;
        ByteBuffer dataBuffer = ByteBuffer.allocate(FILE_SIZE);
        ByteBuffer buffer = ByteBuffer.allocate(FILE_SIZE);
        for (int i = 0; i < FILE_SIZE; i++)
            dataBuffer.put((byte)i);
        dataBuffer.position(0);
        String parentDir = generateFilename();
        simpleDistributedFileSystem.mkdir(parentDir);
        String filename = parentDir + "/" + generateFilename();
        SDFSFileChannel fc = simpleDistributedFileSystem.create(filename);
        fc.write(dataBuffer);
        fc.close();

        fc = simpleDistributedFileSystem.openReadWrite(filename);
        buffer.position(0);
        System.out.println((fc.size() == fileSize) + " " + (fc.getBlockAmount() == getBlockAmount(fileSize)) + " " +
                (fc.position() == 0) + " " + (fc.read(buffer) == fileSize) + " " + equalBuffer(buffer, dataBuffer));


        fc.truncate(0);
        buffer.position(0);

        System.out.println((fc.size() == 0) + " " + (fc.getBlockAmount() == getBlockAmount(0)) + " " +
                (fc.position() == 0) + " " + (fc.read(buffer) == 0));

        SDFSFileChannel fc2 = simpleDistributedFileSystem.openReadonly(filename);
        System.out.println((fc2.size() == fileSize) + " " + (fc2.getBlockAmount() == getBlockAmount(fileSize)) + " " +
                (fc2.position() == 0));

        fc.close();

        System.out.println((fc2.size() == fileSize) + " " + (fc2.getBlockAmount() == getBlockAmount(fileSize)) + " " +
                (fc2.position() == 0));


        fc = simpleDistributedFileSystem.openReadWrite(filename);
        dataBuffer.position(0);

        System.out.println((fc.size() == 0) + " " + (fc.getBlockAmount() == getBlockAmount(0)) + " " +
                (fc.position() == 0) + " " + (fc.read(buffer) == 0) + " " + (fc.write(dataBuffer) == fileSize));

        fc.close();
        fc2.close();
    }

    private static int getBlockAmount(int fileSize) {
        return fileSize == 0 ? 0 : (((fileSize - 1) / Constants.DEFAULT_BLOCK_SIZE) + 1);
    }

    private static String generateFilename() {
        return RandomStringUtils.random(255).replace('/', ':');
    }

    private static boolean equalBuffer(ByteBuffer b1, ByteBuffer b2) {
        boolean result = true;
        if (b1.position() == b2.position() && b1.remaining() == b2.remaining()) {
            while (b1.remaining() > 0) {
                if (b1.get() != b2.get()) {
                    return false;
                }
            }
        }
        else
            result = false;
        return result;
    }
}
