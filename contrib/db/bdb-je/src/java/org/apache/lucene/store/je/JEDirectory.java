package org.apache.lucene.store.je;

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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Port of Andi Vajda's DbDirectory to to Java Edition of Berkeley Database
 * 
 * A JEDirectory is a Berkeley DB JE based implementation of
 * {@link org.apache.lucene.store.Directory Directory}. It uses two
 * {@link com.sleepycat.je.Database Db} database handles, one for storing file
 * records and another for storing file data blocks.
 * 
 * @author Aaron Donovan
 */

public class JEDirectory extends Directory {

    protected Set openFiles = Collections.synchronizedSet(new HashSet());

    protected Database files, blocks;

    protected Transaction txn;

    protected int flags;

    /**
     * Instantiate a DbDirectory. The same threading rules that apply to
     * Berkeley DB handles apply to instances of DbDirectory.
     * 
     * @param txn
     *            a transaction handle that is going to be used for all db
     *            operations done by this instance. This parameter may be
     *            <code>null</code>.
     * @param files
     *            a db handle to store file records.
     * @param blocks
     *            a db handle to store file data blocks.
     * @param flags
     *            flags used for db read operations.
     */

    public JEDirectory(Transaction txn, Database files, Database blocks,
                       int flags) {
        super();

        this.txn = txn;
        this.files = files;
        this.blocks = blocks;
        this.flags = flags;
    }

    public JEDirectory(Transaction txn, Database files, Database blocks) {
        this(txn, files, blocks, 0);
    }

    public void close() throws IOException {
        flush();
    }

    /**
     * Flush the currently open files. After they have been flushed it is safe
     * to commit the transaction without closing this DbDirectory instance
     * first.
     * 
     * @see setTransaction
     */
    public void flush() throws IOException {
        Iterator iterator = openFiles.iterator();

        while (iterator.hasNext()) {
            System.out
                    .println(((JEIndexOutput) iterator.next()).file.getName());
            // ((IndexOutput) iterator.next()).flush();
        }
    }

    public IndexOutput createOutput(String name) throws IOException {
        return new JEIndexOutput(this, name, true);
    }

    public void deleteFile(String name) throws IOException {
        new File(name).delete(this);
    }

    public boolean fileExists(String name) throws IOException {
        return new File(name).exists(this);
    }

    public long fileLength(String name) throws IOException {
        File file = new File(name);

        if (file.exists(this))
            return file.getLength();

        throw new IOException("File does not exist: " + name);
    }

    public long fileModified(String name) throws IOException {
        File file = new File(name);

        if (file.exists(this))
            return file.getTimeModified();

        throw new IOException("File does not exist: " + name);
    }

    public String[] list() throws IOException {
        Cursor cursor = null;
        List list = new ArrayList();

        try {
            try {
                DatabaseEntry key = new DatabaseEntry(new byte[0]);
                DatabaseEntry data = new DatabaseEntry(null);

                data.setPartial(true);
                // TODO see if cursor needs configuration
                cursor = files.openCursor(txn, null);
                // TODO see if LockMode should be set
                if (cursor.getNext(key, data, null) != OperationStatus.NOTFOUND) {
                    ByteArrayInputStream buffer = new ByteArrayInputStream(key
                            .getData());
                    DataInputStream in = new DataInputStream(buffer);
                    String name = in.readUTF();

                    in.close();
                    list.add(name);

                    while (cursor.getNext(key, data, null) != OperationStatus.NOTFOUND) {
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
        } catch (DatabaseException e) {
            throw new IOException(e.getMessage());
        }

        return (String[]) list.toArray(new String[list.size()]);
    }

    public IndexInput openInput(String name) throws IOException {
        return new JEIndexInput(this, name);
    }

    public Lock makeLock(String name) {
        return new JELock();
    }

    public void renameFile(String from, String to) throws IOException {
        new File(from).rename(this, to);
    }

    public void touchFile(String name) throws IOException {
        File file = new File(name);
        long length = 0L;

        if (file.exists(this))
            length = file.getLength();

        file.modify(this, length, System.currentTimeMillis());
    }

    /**
     * Once a transaction handle was committed it is no longer valid. In order
     * to continue using this JEDirectory instance after a commit, the
     * transaction handle has to be replaced.
     * 
     * @param txn
     *            the new transaction handle to use
     */
    public void setTransaction(Transaction txn) {
        this.txn = txn;
    }
}
