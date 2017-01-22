/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.Constants;
import sdfs.datanode.SDFSSlave;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class LocatedBlock implements Serializable {
    private static final long serialVersionUID = -6509598325324530684L;
    public InetAddress inetAddress;
    public int blockNumber;
    public int port = Constants.DEFAULT_PORT + 1;
    public final String serviceName = Constants.DEFAULT_DATA_SERVICE_NAME;
    public int size;
    private long offset;
    private long seq;
    //TODO
    public LocatedBlock() throws UnknownHostException {
        this.inetAddress = InetAddress.getByName(Constants.DEFAULT_IP);
        blockNumber = SDFSSlave.maxBlockId;
        SDFSSlave.maxBlockId ++;
    }
    public LocatedBlock(int blockNumber) throws UnknownHostException {
        this.inetAddress = InetAddress.getByName(Constants.DEFAULT_IP);
        this.blockNumber = blockNumber;
        if (blockNumber >= SDFSSlave.maxBlockId)
            SDFSSlave.maxBlockId = blockNumber + 1;
    }
    public LocatedBlock(InetAddress inetAddress, int blockNumber) {
        this.inetAddress = inetAddress;
        this.blockNumber = blockNumber;
        if (blockNumber >= SDFSSlave.maxBlockId)
            SDFSSlave.maxBlockId = blockNumber + 1;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setAll(LocatedBlock all) {
        this.size = all.size;
        this.offset = all.offset;
        this.inetAddress = all.inetAddress;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocatedBlock that = (LocatedBlock) o;

        return blockNumber == that.blockNumber && inetAddress.equals(that.inetAddress);
    }

    @Override
    public int hashCode() {
        int result = inetAddress.hashCode();
        result = 31 * result + blockNumber;
        return result;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(int blockNumber) {
        this.blockNumber = blockNumber;
    }
}
