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

import org.apache.lucene.store.InputStream;

import com.sleepycat.db.internal.Db;
import com.sleepycat.db.internal.DbTxn;

/**
 * @author Andi Vajda
 */

public class DbInputStream extends InputStream {

    protected long position = 0L;
    protected File file;
    protected Block block;
    protected DbTxn txn;
    protected Db files, blocks;
    protected int flags;

    protected DbInputStream(Db files, Db blocks, DbTxn txn, int flags,
                            String name)
        throws IOException
    {
        super();

        this.files = files;
        this.blocks = blocks;
        this.txn = txn;
        this.flags = flags;

        this.file = new File(name);
        if (!file.exists(files, txn, flags))
            throw new IOException("File does not exist: " + name);

        length = file.getLength();

        block = new Block(file);
        block.get(blocks, txn, flags);
    }

    public Object clone()
    {
        try {
            DbInputStream clone = (DbInputStream) super.clone();

            clone.block = new Block(file);
            clone.block.seek(position);
            clone.block.get(blocks, txn, flags);

            return clone;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void close()
        throws IOException
    {
    }

    protected void readInternal(byte[] b, int offset, int len)
        throws IOException
    {
        int blockPos = (int) (position & DbOutputStream.BLOCK_MASK);

        if (position + len > length)
            throw new IOException("Reading past end of file");

        while (blockPos + len >= DbOutputStream.BLOCK_LEN) {
            int blockLen = DbOutputStream.BLOCK_LEN - blockPos;

            System.arraycopy(block.getData(), blockPos, b, offset, blockLen);

            len -= blockLen;
            offset += blockLen;
            position += blockLen;

            block.seek(position);
            block.get(blocks, txn, flags);
            blockPos = 0;
        }

        if (len > 0)
        {
            System.arraycopy(block.getData(), blockPos, b, offset, len);
            position += len;
        }
    }

    protected void seekInternal(long pos)
        throws IOException
    {
        if (pos > length)
            throw new IOException("seeking past end of file");

        if ((pos >>> DbOutputStream.BLOCK_SHIFT) !=
            (position >>> DbOutputStream.BLOCK_SHIFT))
        {
            block.seek(pos);
            block.get(blocks, txn, flags);
        }

        position = pos;
    }
}
