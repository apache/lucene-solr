package org.apache.lucene.store.db;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by the Open Source
 * Applications Foundation on behalf of the Apache Software Foundation.
 * For more information on the Open Source Applications Foundation, please see
 * <http://www.osafoundation.org>.
 * For more information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;

import org.apache.lucene.store.OutputStream;

import com.sleepycat.db.Db;
import com.sleepycat.db.DbTxn;

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
