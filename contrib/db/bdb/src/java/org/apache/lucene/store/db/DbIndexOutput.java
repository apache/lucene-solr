package org.apache.lucene.store.db;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.lucene.store.IndexOutput;




public class DbIndexOutput extends IndexOutput {

    /**
     * The size of data blocks, currently 16k (2^14), is determined by this
     * constant.
     */
    static public final int BLOCK_SHIFT = 14;
    static public final int BLOCK_LEN = 1 << BLOCK_SHIFT;
    static public final int BLOCK_MASK = BLOCK_LEN - 1;

    protected long position = 0L, length = 0L;
    protected DbDirectory directory;
    protected Block block;
    protected File file;

    protected DbIndexOutput(DbDirectory directory, String name, boolean create)
        throws IOException
    {
        super();

        this.directory = directory;

        file = new File(directory, name, create);
        block = new Block(file);
        length = file.getLength();

        seek(length);
        block.get(directory);

        directory.openFiles.add(this);
    }

    public void close()
        throws IOException
    {
        flush();
        file.modify(directory, length, System.currentTimeMillis());

        directory.openFiles.remove(this);
    }

    public void flush()
        throws IOException
    {
        if (length > 0)
            block.put(directory);
    }

    public void writeByte(byte b)
        throws IOException
    {
        int blockPos = (int) (position++ & BLOCK_MASK);

        block.getData()[blockPos] = b;

        if (blockPos + 1 == BLOCK_LEN)
        {
            block.put(directory);
            block.seek(position);
            block.get(directory);
        }

        if (position > length)
            length = position;
    }

    public void writeBytes(byte[] b, int offset, int len)
        throws IOException
    {
        int blockPos = (int) (position & BLOCK_MASK);

        while (blockPos + len >= BLOCK_LEN) {
            int blockLen = BLOCK_LEN - blockPos;

            System.arraycopy(b, offset, block.getData(), blockPos, blockLen);
            block.put(directory);

            len -= blockLen;
            offset += blockLen;
            position += blockLen;

            block.seek(position);
            block.get(directory);
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
        if (pos > length)
            throw new IOException("seeking past end of file");

        if ((pos >>> BLOCK_SHIFT) == (position >>> BLOCK_SHIFT))
            position = pos;
        else
        {
            block.put(directory);
            block.seek(pos);
            block.get(directory);
            position = pos;
        }
    }

    public long getFilePointer()
    {
        return position;
    }
}
