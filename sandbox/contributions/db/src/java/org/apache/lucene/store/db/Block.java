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

import com.sleepycat.db.Dbt;
import com.sleepycat.db.Db;
import com.sleepycat.db.DbTxn;
import com.sleepycat.db.DbException;

/**
 * @author Andi Vajda
 */

public class Block extends Object {
    protected Dbt key, data;

    protected Block(File file)
        throws IOException
    {
        byte[] fileKey = file.getKey();

        key = new Dbt(new byte[fileKey.length + 8]);
        key.setFlags(Db.DB_DBT_USERMEM);

        data = new Dbt(new byte[DbOutputStream.BLOCK_LEN]);
        data.setUserBufferLength(data.getSize());
        data.setFlags(Db.DB_DBT_USERMEM);

        System.arraycopy(fileKey, 0, key.getData(), 0, fileKey.length);
        seek(0L);
    }

    protected byte[] getKey()
    {
        return key.getData();
    }

    protected byte[] getData()
    {
        return data.getData();
    }

    protected void seek(long position)
        throws IOException
    {
        position = position >>> DbOutputStream.BLOCK_SHIFT;
        byte[] data = key.getData();
        int last = data.length - 1;

        for (int i = 0; i < 8; i++) {
            data[last - i] = (byte) (position & 0xff);
            position >>>= 8;
        }
    }

    protected void get(Db blocks, DbTxn txn)
        throws IOException
    {
        try {
            blocks.get(txn, key, data, 0);
        } catch (DbException e) {
            throw new IOException(e.getMessage());
        }
    }

    protected void put(Db blocks, DbTxn txn)
        throws IOException
    {
        try {
            blocks.put(txn, key, data, 0);
        } catch (DbException e) {
            throw new IOException(e.getMessage());
        }
    }
}
