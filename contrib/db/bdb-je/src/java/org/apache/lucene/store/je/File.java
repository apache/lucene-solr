package org.apache.lucene.store.je;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Port of Andi Vajda's DbDirectory to Java Edition of Berkeley Database
 *
 */

public class File extends Object {

    static protected Random random = new Random();

    protected DatabaseEntry key, data;

    protected long length, timeModified;

    protected String name;

    protected byte[] uuid;

    protected File(String name) throws IOException {
        setName(name);

        data = new DatabaseEntry(new byte[32]);
    }

    protected File(JEDirectory directory, String name, boolean create)
            throws IOException {
        this(name);

        if (!exists(directory)) {
            if (!create)
                throw new IOException("File does not exist: " + name);
            else {
                DatabaseEntry key = new DatabaseEntry(new byte[24]);
                DatabaseEntry data = new DatabaseEntry(null);
                Database blocks = directory.blocks;
                Transaction txn = directory.txn;

                data.setPartial(true);

                uuid = new byte[16];

                try {
                    do {
                        /* generate a v.4 random-uuid unique to this db */
                        random.nextBytes(uuid);
                        uuid[6] = (byte) ((byte) 0x40 | (uuid[6] & (byte) 0x0f));
                        uuid[8] = (byte) ((byte) 0x80 | (uuid[8] & (byte) 0x3f));
                        System.arraycopy(uuid, 0, key.getData(), 0, 16);
                        // TODO check LockMode
                    } while (blocks.get(txn, key, data, null) != OperationStatus.NOTFOUND);
                } catch (DatabaseException e) {
                    throw new IOException(e.getMessage());
                }
            }
        } else if (create)
            length = 0L;
    }

    protected String getName() {
        return name;
    }

    private void setName(String name) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(128);
        DataOutputStream out = new DataOutputStream(buffer);

        out.writeUTF(name);
        out.close();

        key = new DatabaseEntry(buffer.toByteArray());
        this.name = name;
    }

    protected byte[] getKey() throws IOException {
        if (uuid == null)
            throw new IOException("Uninitialized file");

        return uuid;
    }

    protected long getLength() {
        return length;
    }

    protected long getTimeModified() {
        return timeModified;
    }

    protected boolean exists(JEDirectory directory) throws IOException {
        Database files = directory.files;
        Transaction txn = directory.txn;
        try {
            // TODO check LockMode
            if (files.get(txn, key, data, null) == OperationStatus.NOTFOUND)
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

    protected void modify(JEDirectory directory, long length, long timeModified)
            throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(32);
        DataOutputStream out = new DataOutputStream(buffer);
        Database files = directory.files;
        Transaction txn = directory.txn;

        out.writeLong(length);
        out.writeLong(timeModified);
        out.write(getKey());
        out.close();

        System.arraycopy(buffer.toByteArray(), 0, data.getData(), 0, 32);

        try {
            files.put(txn, key, data);
        } catch (DatabaseException e) {
            throw new IOException(e.getMessage());
        }

        this.length = length;
        this.timeModified = timeModified;
    }

    protected void delete(JEDirectory directory) throws IOException {
        if (!exists(directory))
            throw new IOException("File does not exist: " + getName());

        Cursor cursor = null;

        try {
            try {
                byte[] bytes = getKey();
                int ulen = bytes.length + 8;
                byte[] cursorBytes = new byte[ulen];
                DatabaseEntry cursorKey = new DatabaseEntry(cursorBytes);
                DatabaseEntry cursorData = new DatabaseEntry(null);
                Database files = directory.files;
                Database blocks = directory.blocks;
                Transaction txn = directory.txn;

                System.arraycopy(bytes, 0, cursorBytes, 0, bytes.length);

                cursorData.setPartial(true);

                cursor = blocks.openCursor(txn, null);

                if (cursor.getSearchKey(cursorKey, cursorData, null) != OperationStatus.NOTFOUND) {
                    cursor.delete();
                    advance: while (cursor.getNext(cursorKey, cursorData, null) != OperationStatus.NOTFOUND) {
                        byte[] temp = cursorKey.getData();
                        for (int i = 0; i < bytes.length; i++)
                            if (bytes[i] != temp[i]) {
                                break advance;
                            }
                        cursor.delete();
                    }
                }

                files.delete(txn, key);
            } finally {
                if (cursor != null)
                    cursor.close();
            }
        } catch (DatabaseException e) {
            throw new IOException(e.getMessage());
        }

    }

    protected void rename(JEDirectory directory, String name)
            throws IOException {
        if (!exists(directory))
            throw new IOException("File does not exist: " + getName());

        File newFile = new File(name);

        if (newFile.exists(directory))
            newFile.delete(directory);

        try {
            Database files = directory.files;
            Transaction txn = directory.txn;

            files.delete(txn, key);
            setName(name);
            files.put(txn, key, data);
        } catch (DatabaseException e) {
            throw new IOException(e.getMessage());
        }
    }
}
