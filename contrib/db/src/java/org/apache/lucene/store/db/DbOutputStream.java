package org.apache.lucene.store.db;

/**
 * Copyright 2002-2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.store.OutputStream;

import com.sleepycat.db.internal.Db;
import com.sleepycat.db.internal.DbTxn;

/**
 * @author Andi Vajda
 */

public class DbOutputStream extends OutputStream {

    /**
     * The size of data blocks, currently 16k (2^14), is determined by this
     * constant.
     */
    static public final int BLOCK_SHIFT = 14;
    static public final int BLOCK_LEN = 1 << BLOCK_SHIFT;
    static public final int BLOCK_MASK = BLOCK_LEN - 1;

    protected long position = 0L, length = 0L;
    protected File file;
    protected Block block;
    protected DbTxn txn;
    protected Db files, blocks;
    protected int flags;

    protected DbOutputStream(Db files, Db blocks, DbTxn txn, int flags,
                             String name, boolean create)
        throws IOException
    {
        super();

        this.files = files;
        this.blocks = blocks;
        this.txn = txn;
        this.flags = flags;

        file = new File(files, blocks, txn, flags, name, create);
        block = new Block(file);
        length = file.getLength();

        seek(length);
        block.get(blocks, txn, flags);
    }

    public void close()
        throws IOException
    {
        flush();
        if (length > 0)
            block.put(blocks, txn, flags);

        file.modify(files, txn, flags, length, System.currentTimeMillis());
    }

    protected void flushBuffer(byte[] b, int len)
        throws IOException
    {
        int blockPos = (int) (position & BLOCK_MASK);
        int offset = 0;

        while (blockPos + len >= BLOCK_LEN) {
            int blockLen = BLOCK_LEN - blockPos;

            System.arraycopy(b, offset, block.getData(), blockPos, blockLen);
            block.put(blocks, txn, flags);

            len -= blockLen;
            offset += blockLen;
            position += blockLen;

            block.seek(position);
            block.get(blocks, txn, flags);
            blockPos = 0;
        }

        if (len > 0)
        {
            System.arraycopy(b, offset, block.getData(), blockPos, len);
            position += len;
        }

        if (position > length)
            length = position;
    }

    public long length()
        throws IOException
    {
        return length;
    }

    public void seek(long pos)
        throws IOException
    {
        super.seek(pos);
        seekInternal(pos);
    }

    protected void seekInternal(long pos)
        throws IOException
    {
        if (pos > length)
            throw new IOException("seeking past end of file");

        if ((pos >>> BLOCK_SHIFT) == (position >>> BLOCK_SHIFT))
            position = pos;
        else
        {
            block.put(blocks, txn, flags);
            block.seek(pos);
            block.get(blocks, txn, flags);
            position = pos;
        }
    }
}
