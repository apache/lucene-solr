package org.apache.lucene.store.db;

/**
 * Copyright 2002-2006 The Apache Software Foundation
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
import org.apache.lucene.store.IndexInput;


/**
 * @author Andi Vajda
 */

public class DbIndexInput extends IndexInput {

    protected long position = 0L, length = 0L;
    protected DbDirectory directory;
    protected Block block;
    protected File file;

    protected DbIndexInput(DbDirectory directory, String name)
        throws IOException
    {
        super();

        this.directory = directory;

        this.file = new File(name);
        if (!file.exists(directory))
            throw new IOException("File does not exist: " + name);

        length = file.getLength();

        block = new Block(file);
        block.get(directory);
    }

    public Object clone()
    {
        try {
            DbIndexInput clone = (DbIndexInput) super.clone();

            clone.block = new Block(file);
            clone.block.seek(position);
            clone.block.get(directory);

            return clone;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void close()
        throws IOException
    {
    }

    public long length()
    {
        return length;
    }

    public byte readByte()
        throws IOException
    {
        if (position + 1 > length)
            throw new IOException("Reading past end of file");

        int blockPos = (int) (position++ & DbIndexOutput.BLOCK_MASK);
        byte b = block.getData()[blockPos];

        if (blockPos + 1 == DbIndexOutput.BLOCK_LEN)
        {
            block.seek(position);
            block.get(directory);
        }

        return b;
    }

    public void readBytes(byte[] b, int offset, int len)
        throws IOException
    {
        if (position + len > length)
            throw new IOException("Reading past end of file");
        else
        {
            int blockPos = (int) (position & DbIndexOutput.BLOCK_MASK);

            while (blockPos + len >= DbIndexOutput.BLOCK_LEN) {
                int blockLen = DbIndexOutput.BLOCK_LEN - blockPos;

                System.arraycopy(block.getData(), blockPos,
                                 b, offset, blockLen);

                len -= blockLen;
                offset += blockLen;
                position += blockLen;

                block.seek(position);
                block.get(directory);
                blockPos = 0;
            }

            if (len > 0)
            {
                System.arraycopy(block.getData(), blockPos, b, offset, len);
                position += len;
            }
        }
    }

    public void seek(long pos)
        throws IOException
    {
        if (pos > length)
            throw new IOException("seeking past end of file");

        if ((pos >>> DbIndexOutput.BLOCK_SHIFT) !=
            (position >>> DbIndexOutput.BLOCK_SHIFT))
        {
            block.seek(pos);
            block.get(directory);
        }

        position = pos;
    }

    public long getFilePointer()
    {
        return position;
    }
}
