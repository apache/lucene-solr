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
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.InputStream;

import com.sleepycat.db.DbEnv;
import com.sleepycat.db.Db;
import com.sleepycat.db.Dbt;
import com.sleepycat.db.Dbc;
import com.sleepycat.db.DbTxn;
import com.sleepycat.db.DbException;

/**
 * A DbDirectory is a Berkeley DB 4.2 based implementation of 
 * {@link org.apache.lucene.store.Directory Directory}. It uses two
 * {@link com.sleepycat.db.Db Db} database handles, one for storing file
 * records and another for storing file data blocks.
 *
 * @author Andi Vajda
 */

public class DbDirectory extends Directory {

    protected Db files, blocks;
    protected DbTxn txn;

    /**
     * Instantiate a DbDirectory. The same threading rules that apply to
     * Berkeley DB handles apply to instances of DbDirectory.
     *
     * @param txn a transaction handle that is going to be used for all db
     * operations done by this instance. This parameter may be
     * <code>null</code>.
     * @param files a db handle to store file records.
     * @param blocks a db handle to store file data blocks.
     */

    public DbDirectory(DbTxn txn, Db files, Db blocks)
    {
        super();

        this.txn = txn;
        this.files = files;
        this.blocks = blocks;
    }

    public void close()
        throws IOException
    {
    }

    public OutputStream createFile(String name)
        throws IOException
    {
        return new DbOutputStream(files, blocks, txn, name, true);
    }

    public void deleteFile(String name)
        throws IOException
    {
        new File(name).delete(files, blocks, txn);
    }

    public boolean fileExists(String name)
        throws IOException
    {
        return new File(name).exists(files, txn);
    }

    public long fileLength(String name)
        throws IOException
    {
        File file = new File(name);

        if (file.exists(files, txn))
            return file.getLength();

        throw new IOException("File does not exist: " + name);
    }
    
    public long fileModified(String name)
        throws IOException
    {
        File file = new File(name);

        if (file.exists(files, txn))
            return file.getTimeModified();

        throw new IOException("File does not exist: " + name);
    }

    public String[] list()
        throws IOException
    {
        Dbc cursor = null;
        List list = new ArrayList();

        try {
            try {
                Dbt key = new Dbt(new byte[0]);
                Dbt data = new Dbt(null);

                data.setPartialLength(0);
                data.setFlags(Db.DB_DBT_PARTIAL);

                cursor = files.cursor(txn, 0);

                if (cursor.get(key, data, Db.DB_SET_RANGE) != Db.DB_NOTFOUND)
                {
                    ByteArrayInputStream buffer =
                        new ByteArrayInputStream(key.getData());
                    DataInputStream in = new DataInputStream(buffer);
                    String name = in.readUTF();
                
                    in.close();
                    list.add(name);

                    while (cursor.get(key, data, Db.DB_NEXT) != Db.DB_NOTFOUND) {
                        buffer = new ByteArrayInputStream(key.getData());
                        in = new DataInputStream(buffer);
                        name = in.readUTF();
                        in.close();

                        list.add(name);
                    }
                }
            } finally {
                if (cursor != null)
                    cursor.close();
            }
        } catch (DbException e) {
            throw new IOException(e.getMessage());
        }

        return (String[]) list.toArray(new String[list.size()]);
    }

    public InputStream openFile(String name)
        throws IOException
    {
        return new DbInputStream(files, blocks, txn, name);
    }

    public Lock makeLock(String name)
    {
        return new DbLock();
    }

    public void renameFile(String from, String to)
        throws IOException
    {
        new File(from).rename(files, blocks, txn, to);
    }

    public void touchFile(String name)
        throws IOException
    {
        File file = new File(name);
        long length = 0L;

        if (file.exists(files, txn))
            length = file.getLength();

        file.modify(files, txn, length, System.currentTimeMillis());
    }
}
