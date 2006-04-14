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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Random;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.internal.DbConstants;
import com.sleepycat.db.internal.Dbc;
import com.sleepycat.db.internal.Db;
import com.sleepycat.db.internal.DbTxn;
import com.sleepycat.db.DatabaseException;

/**
 * @author Andi Vajda
 */

public class File extends Object {

    static protected Random random = new Random();

    protected DatabaseEntry key, data;
    protected long length, timeModified;
    protected String name;
    protected byte[] uuid;

    protected File(String name)
        throws IOException
    {
        setName(name);

        data = new DatabaseEntry(new byte[32]);
        data.setUserBuffer(data.getSize(), true);
    }

    protected File(DbDirectory directory, String name, boolean create)
        throws IOException
    {
        this(name);

        if (!exists(directory))
        {
            if (!create)
                throw new IOException("File does not exist: " + name);
            else
            {
                DatabaseEntry key = new DatabaseEntry(new byte[24]);
                DatabaseEntry data = new DatabaseEntry(null);
                Db blocks = directory.blocks;
                DbTxn txn = directory.txn;
                int flags = directory.flags;

                key.setUserBuffer(24, true);
                data.setPartial(true);

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
                                        flags) != DbConstants.DB_NOTFOUND);
                } catch (DatabaseException e) {
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

        key = new DatabaseEntry(buffer.toByteArray());
        key.setUserBuffer(key.getSize(), true);

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

    protected boolean exists(DbDirectory directory)
        throws IOException
    {
        Db files = directory.files;
        DbTxn txn = directory.txn;
        int flags = directory.flags;

        try {
            if (files.get(txn, key, data, flags) == DbConstants.DB_NOTFOUND)
                return false;
        } catch (DatabaseException e) {
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

    protected void modify(DbDirectory directory, long length, long timeModified)
        throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(32);
        DataOutputStream out = new DataOutputStream(buffer);
        Db files = directory.files;
        DbTxn txn = directory.txn;

        out.writeLong(length);
        out.writeLong(timeModified);
        out.write(getKey());
        out.close();

        System.arraycopy(buffer.toByteArray(), 0, data.getData(), 0, 32);

        try {
            files.put(txn, key, data, 0);
        } catch (DatabaseException e) {
            throw new IOException(e.getMessage());
        }
        
        this.length = length;
        this.timeModified = timeModified;
    }

    protected void delete(DbDirectory directory)
        throws IOException
    {
        if (!exists(directory))
            throw new IOException("File does not exist: " + getName());

        Dbc cursor = null;

        try {
            try {
                byte[] bytes = getKey();
                int ulen = bytes.length + 8;
                byte[] cursorBytes = new byte[ulen];
                DatabaseEntry cursorKey = new DatabaseEntry(cursorBytes);
                DatabaseEntry cursorData = new DatabaseEntry(null);
                Db files = directory.files;
                Db blocks = directory.blocks;
                DbTxn txn = directory.txn;
                int flags = directory.flags;

                System.arraycopy(bytes, 0, cursorBytes, 0, bytes.length);
                cursorKey.setUserBuffer(ulen, true);
                cursorData.setPartial(true);

                cursor = blocks.cursor(txn, flags);

                if (cursor.get(cursorKey, cursorData,
                               DbConstants.DB_SET_RANGE | flags) != DbConstants.DB_NOTFOUND)
                {
                    cursor.del(0);

                  outer:
                    while (cursor.get(cursorKey, cursorData,
                                      DbConstants.DB_NEXT | flags) != DbConstants.DB_NOTFOUND)
                    {
                        for (int i = 0; i < bytes.length; i++)
                            if (bytes[i] != cursorBytes[i])
                                break outer;

                        cursor.del(0);
                    }
                }

                files.del(txn, key, 0);
            } finally {
                if (cursor != null)
                    cursor.close();
            }
        } catch (DatabaseException e) {
            throw new IOException(e.getMessage());
        }
    }

    protected void rename(DbDirectory directory, String name)
        throws IOException
    {
        if (!exists(directory))
            throw new IOException("File does not exist: " + getName());

        File newFile = new File(name);

        if (newFile.exists(directory))
            newFile.delete(directory);

        try {
            Db files = directory.files;
            DbTxn txn = directory.txn;

            files.del(txn, key, 0);
            setName(name);
            files.put(txn, key, data, 0);
        } catch (DatabaseException e) {
            throw new IOException(e.getMessage());
        }
    }
}
