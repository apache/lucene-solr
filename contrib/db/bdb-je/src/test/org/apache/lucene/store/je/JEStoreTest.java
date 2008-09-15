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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Tests {@link JEDirectory}.
 * 
 * Adapted from Andi Vajda's org.apache.lucene.db.DbStoreTest.
 *
 */
public class JEStoreTest extends TestCase {
    protected File dbHome = new File(System.getProperty("java.io.tmpdir"),"index");

    protected Environment env;

    protected Database index, blocks;

    public void setUp() throws Exception {

        if (!dbHome.exists())
            dbHome.mkdir();
        else {
            File[] files = dbHome.listFiles();

            for (int i = 0; i < files.length; i++) {
                String name = files[i].getName();
                if (name.endsWith("jdb") || name.equals("je.lck"))
                    files[i].delete();
            }
        }

        EnvironmentConfig envConfig = new EnvironmentConfig();
        DatabaseConfig dbConfig = new DatabaseConfig();

        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        env = new Environment(dbHome, envConfig);

        Transaction txn = null;

        try {
            txn = env.beginTransaction(null, null);
            index = env.openDatabase(txn, "__index__", dbConfig);
            blocks = env.openDatabase(txn, "__blocks__", dbConfig);
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            index = null;
            blocks = null;
            throw e;
        } finally {
            if (txn != null)
                txn.commit();
            txn = null;
        }
    }

    public void tearDown() throws Exception {

        if (index != null)
            index.close();
        if (blocks != null)
            blocks.close();
        if (env != null)
            env.close();
    }

    public void tesBytes() throws Exception {
        final int count = 250;
        final int LENGTH_MASK = 0xffff;

        Random gen = new Random(1251971);
        int totalLength = 0;
        int duration;
        Date end;

        Date veryStart = new Date();
        Date start = new Date();
        Transaction txn = null;
        Directory store = null;

        System.out.println("Writing files byte by byte");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexOutput file = store.createOutput(name);

                totalLength += length;

                for (int j = 0; j < length; j++) {
                    byte b = (byte) (gen.nextInt() & 0x7F);
                    file.writeByte(b);
                }

                file.close();
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        duration = (int) (end.getTime() - start.getTime());
        System.out.print(duration);
        System.out.print(" total milliseconds to create, ");
        System.out.print(totalLength / duration);
        System.out.println(" kb/s");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            gen = new Random(1251971);
            start = new Date();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexInput file = store.openInput(name);

                if (file.length() != length)
                    throw new Exception("length incorrect");

                for (int j = 0; j < length; j++) {
                    byte b = (byte) (gen.nextInt() & 0x7F);

                    if (file.readByte() != b)
                        throw new Exception("contents incorrect");
                }

                file.close();
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        duration = (int) (end.getTime() - start.getTime());
        System.out.print(duration);
        System.out.print(" total milliseconds to read, ");
        System.out.print(totalLength / duration);
        System.out.println(" kb/s");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            gen = new Random(1251971);
            start = new Date();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                store.deleteFile(name);
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        System.out.print(end.getTime() - start.getTime());
        System.out.println(" total milliseconds to delete");

        System.out.print(end.getTime() - veryStart.getTime());
        System.out.println(" total milliseconds");
    }

    public void testDelete() throws Exception {
        final int count = 250;
        final int LENGTH_MASK = 0xffff;

        Random gen = new Random(1251971);
        int totalLength = 0;
        int duration;
        Date end;

        Date veryStart = new Date();
        Date start = new Date();
        Transaction txn = null;
        Directory store = null;

        System.out.println("Writing files byte by byte");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexOutput file = store.createOutput(name);

                totalLength += length;

                for (int j = 0; j < length; j++) {
                    byte b = (byte) (gen.nextInt() & 0x7F);
                    file.writeByte(b);
                }

                file.close();
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        duration = (int) (end.getTime() - start.getTime());
        System.out.print(duration);
        System.out.print(" total milliseconds to read, ");
        System.out.print(totalLength / duration);
        System.out.println(" kb/s");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            gen = new Random(1251971);
            start = new Date();

            for (int i = 0; i < count; i++) {
                if (i % 2 == 0) {
                    String name = i + ".dat";
                    store.deleteFile(name);
                }
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        System.out.print(end.getTime() - start.getTime());
        System.out.println(" total milliseconds to delete even files");

        duration = (int) (end.getTime() - start.getTime());
        System.out.print(duration);
        System.out.print(" total milliseconds to create, ");
        System.out.print(totalLength / duration);
        System.out.println(" kb/s");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            gen = new Random(1251971);
            start = new Date();

            for (int i = 0; i < count; i++) {
                int length = gen.nextInt() & LENGTH_MASK;

                if (i % 2 != 0) {
                    String name = i + ".dat";
                    IndexInput file = store.openInput(name);
                    if (file.length() != length)
                        throw new Exception("length incorrect");

                    for (int j = 0; j < length; j++) {
                        byte b = (byte) (gen.nextInt() & 0x7F);

                        if (file.readByte() != b)
                            throw new Exception("contents incorrect");
                    }

                    file.close();
                } else {
                    for (int j = 0; j < length; j++) {
                        gen.nextInt();
                    }
                }
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        duration = (int) (end.getTime() - start.getTime());
        System.out.print(duration);
        System.out.print(" total milliseconds to read, ");
        System.out.print(totalLength / duration);
        System.out.println(" kb/s");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            gen = new Random(1251971);
            start = new Date();

            for (int i = 0; i < count; i++) {
                if (i % 2 != 0) {
                    String name = i + ".dat";
                    store.deleteFile(name);
                }
            }

        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        System.out.print(end.getTime() - start.getTime());
        System.out.println(" total milliseconds to delete");

        System.out.print(end.getTime() - veryStart.getTime());
        System.out.println(" total milliseconds");

        Cursor cursor = null;
        try {
            cursor = index.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            if (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                fail("index database is not empty");
            }
        } catch (DatabaseException e) {
            throw e;
        } finally {
            if (cursor != null)
                cursor.close();
        }

        cursor = null;
        try {
            cursor = blocks.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            if (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                fail("blocks database is not empty");
            }
        } catch (DatabaseException e) {
            throw e;
        } finally {
            if (cursor != null)
                cursor.close();
        }
    }

    public void tesArrays() throws Exception {
        final int count = 250;
        final int LENGTH_MASK = 0xffff;

        Random gen = new Random(1251971);
        int totalLength = 0;
        int duration;
        Date end;

        Date veryStart = new Date();
        Date start = new Date();
        Transaction txn = null;
        Directory store = null;

        System.out.println("Writing files as one byte array");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexOutput file = store.createOutput(name);
                byte[] data = new byte[length];

                totalLength += length;
                gen.nextBytes(data);
                file.writeBytes(data, length);

                file.close();
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        duration = (int) (end.getTime() - start.getTime());
        System.out.print(duration);
        System.out.print(" total milliseconds to create, ");
        System.out.print(totalLength / duration);
        System.out.println(" kb/s");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            gen = new Random(1251971);
            start = new Date();

            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                int length = gen.nextInt() & LENGTH_MASK;
                IndexInput file = store.openInput(name);

                if (file.length() != length)
                    throw new Exception("length incorrect");

                byte[] data = new byte[length];
                byte[] read = new byte[length];
                gen.nextBytes(data);
                file.readBytes(read, 0, length);

                if (!Arrays.equals(data, read))
                    throw new Exception("contents incorrect");

                file.close();
            }
        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        duration = (int) (end.getTime() - start.getTime());
        System.out.print(duration);
        System.out.print(" total milliseconds to read, ");
        System.out.print(totalLength / duration);
        System.out.println(" kb/s");

        try {
            txn = env.beginTransaction(null, null);
            store = new JEDirectory(txn, index, blocks);

            gen = new Random(1251971);
            start = new Date();
            for (int i = 0; i < count; i++) {
                String name = i + ".dat";
                store.deleteFile(name);
            }

        } catch (IOException e) {
            txn.abort();
            txn = null;
            throw e;
        } catch (DatabaseException e) {
            if (txn != null) {
                txn.abort();
                txn = null;
            }
            throw e;
        } finally {
            if (txn != null)
                txn.commit();

            store.close();
        }

        end = new Date();

        System.out.print(end.getTime() - start.getTime());
        System.out.println(" total milliseconds to delete");

        System.out.print(end.getTime() - veryStart.getTime());
        System.out.println(" total milliseconds");
    }
}
