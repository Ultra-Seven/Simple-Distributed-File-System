/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import sdfs.namenode.LocatedBlock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BlockInfo implements Serializable, Iterable<LocatedBlock> {
    private static final long serialVersionUID = 8712105981933359634L;
    private final List<LocatedBlock> locatedBlocks = new ArrayList<>();

    @Override
    public Iterator<LocatedBlock> iterator() {
        return locatedBlocks.iterator();
    }

    public boolean addLocatedBlock(LocatedBlock locatedBlock) {
        return locatedBlocks.add(locatedBlock);
    }

    public boolean removeLocatedBlock(LocatedBlock locatedBlock) {
        return locatedBlocks.remove(locatedBlock);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockInfo that = (BlockInfo) o;

        return locatedBlocks.equals(that.locatedBlocks);
    }

    @Override
    public int hashCode() {
        return locatedBlocks.hashCode();
    }

    public List<LocatedBlock> getLocatedBlocks() {
        return locatedBlocks;
    }
    public LocatedBlock getBlock() {
        int index = (int)(Math.random() * locatedBlocks.size());
        return locatedBlocks.get(index);
    }
}
