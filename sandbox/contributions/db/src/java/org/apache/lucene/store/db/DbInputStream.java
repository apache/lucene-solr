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

import org.apache.lucene.store.InputStream;

import com.sleepycat.db.Db;
import com.sleepycat.db.DbTxn;
import com.sleepycat.db.Dbt;
import com.sleepycat.db.DbException;

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
