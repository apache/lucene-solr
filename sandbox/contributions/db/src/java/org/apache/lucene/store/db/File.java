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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Random;

import com.sleepycat.db.Dbt;
import com.sleepycat.db.Dbc;
import com.sleepycat.db.Db;
import com.sleepycat.db.DbTxn;
import com.sleepycat.db.DbException;

/**
 * @author Andi Vajda
 */

public class File extends Object {

    static protected Random random = new Random();

    protected Dbt key, data;
    protected long length, timeModified;
    protected String name;
    protected byte[] uuid;

    protected File(String name)
        throws IOException
    {
        setName(name);

        data = new Dbt(new byte[32]);
        data.setUserBufferLength(data.getSize());
        data.setFlags(Db.DB_DBT_USERMEM);
    }

    protected File(Db files, Db blocks, DbTxn txn, int flags,
                   String name, boolean create)
        throws IOException
    {
        this(name);

        if (!exists(files, txn, flags))
        {
            if (!create)
                throw new IOException("File does not exist: " + name);
            else
            {
                Dbt key = new Dbt(new byte[24]);
                Dbt data = new Dbt(null);

                key.setFlags(Db.DB_DBT_USERMEM);
                key.setUserBufferLength(24);
                data.setPartialLength(0);
                data.setFlags(Db.DB_DBT_USERMEM | Db.DB_DBT_PARTIAL);

                uuid = new byte[16];

                try {
                    do {
                        /* generate a v.4 random-uuid unique to this db */
                        random.nextBytes(uuid);
                        uuid[6] = (byte) ((byte) 0x40 |
                                          (uuid[6] & (byte) 0x0f));
                        uuid[8] = (byte) ((byte) 0x80 |
                                          (uuid[8] & (byte) 0x3f));
                        System.arraycopy(uuid, 0, key.getData(), 0, 16);
                    } while (blocks.get(txn, key, data,
                                        flags) != Db.DB_NOTFOUND);
                } catch (DbException e) {
                    throw new IOException(e.getMessage());
                }
            }
        }
        else if (create)
            length = 0L;
    }

    protected String getName()
    {
        return name;
    }

    private void setName(String name)
        throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(128);
        DataOutputStream out = new DataOutputStream(buffer);

        out.writeUTF(name);
        out.close();

        key = new Dbt(buffer.toByteArray());
        key.setUserBufferLength(key.getSize());
        key.setFlags(Db.DB_DBT_USERMEM);

        this.name = name;
    }

    protected byte[] getKey()
        throws IOException
    {
        if (uuid == null)
            throw new IOException("Uninitialized file");

        return uuid;
    }

    protected long getLength()
    {
        return length;
    }

    protected long getTimeModified()
    {
        return timeModified;
    }

    protected boolean exists(Db files, DbTxn txn, int flags)
        throws IOException
    {
        try {
            if (files.get(txn, key, data, flags) == Db.DB_NOTFOUND)
                return false;
        } catch (DbException e) {
            throw new IOException(e.getMessage());
        }
        
        byte[] bytes = data.getData();
        ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(buffer);

        length = in.readLong();
        timeModified = in.readLong();
        in.close();

        uuid = new byte[16];
        System.arraycopy(bytes, 16, uuid, 0, 16);

        return true;
    }

    protected void modify(Db files, DbTxn txn, int flags,
                          long length, long timeModified)
        throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(32);
        DataOutputStream out = new DataOutputStream(buffer);

        out.writeLong(length);
        out.writeLong(timeModified);
        out.write(getKey());
        out.close();

        System.arraycopy(buffer.toByteArray(), 0, data.getData(), 0, 32);

        try {
            files.put(txn, key, data, 0);
        } catch (DbException e) {
            throw new IOException(e.getMessage());
        }
        
        this.length = length;
        this.timeModified = timeModified;
    }

    protected void delete(Db files, Db blocks, DbTxn txn, int flags)
        throws IOException
    {
        if (!exists(files, txn, flags))
            throw new IOException("File does not exist: " + getName());

        Dbc cursor = null;

        try {
            try {
                byte[] bytes = getKey();
                int ulen = bytes.length + 8;
                byte[] cursorBytes = new byte[ulen];
                Dbt cursorKey = new Dbt(cursorBytes);
                Dbt cursorData = new Dbt(null);

                System.arraycopy(bytes, 0, cursorBytes, 0, bytes.length);
                cursorKey.setUserBufferLength(ulen);
                cursorKey.setFlags(Db.DB_DBT_USERMEM);
                cursorData.setPartialLength(0);
                cursorData.setFlags(Db.DB_DBT_PARTIAL);

                cursor = blocks.cursor(txn, flags);

                if (cursor.get(cursorKey, cursorData,
                               Db.DB_SET_RANGE | flags) != Db.DB_NOTFOUND)
                {
                    cursor.delete(0);

                    while (cursor.get(cursorKey, cursorData,
                                      Db.DB_NEXT | flags) != Db.DB_NOTFOUND) {
                        for (int i = 0; i < bytes.length; i++)
                            if (bytes[i] != cursorBytes[i])
                                return;

                        cursor.delete(0);
                    }
                }

                files.delete(txn, key, 0);
            } finally {
                if (cursor != null)
                    cursor.close();
            }
        } catch (DbException e) {
            throw new IOException(e.getMessage());
        }
    }

    protected void rename(Db files, Db blocks, DbTxn txn, int flags,
                          String name)
        throws IOException
    {
        if (!exists(files, txn, flags))
            throw new IOException("File does not exist: " + getName());

        File newFile = new File(name);

        if (newFile.exists(files, txn, flags))
            newFile.delete(files, blocks, txn, flags);

        try {
            files.delete(txn, key, 0);
            setName(name);
            files.put(txn, key, data, 0);
        } catch (DbException e) {
            throw new IOException(e.getMessage());
        }
    }
}
