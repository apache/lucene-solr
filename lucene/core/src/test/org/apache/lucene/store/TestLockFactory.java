/*
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
package org.apache.lucene.store;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.util.LuceneTestCase;

public class TestLockFactory extends LuceneTestCase {

    // Verify: we can provide our own LockFactory implementation, the right
    // methods are called at the right time, locks are created, etc.

    public void testCustomLockFactory() throws IOException {
        MockLockFactory lf = new MockLockFactory();
        Directory dir = new MockDirectoryWrapper(random(), new RAMDirectory(lf));

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

        // add 100 documents (so that commit lock is used)
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // Both write lock and commit lock should have been created:
        assertEquals("# of unique locks created (after instantiating IndexWriter)",
                     1, lf.locksCreated.size());
        writer.close();
    }

    // Verify: we can use the NoLockFactory with RAMDirectory w/ no
    // exceptions raised:
    // Verify: NoLockFactory allows two IndexWriters
    public void testRAMDirectoryNoLocking() throws IOException {
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new RAMDirectory(NoLockFactory.INSTANCE));

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));
        writer.commit(); // required so the second open succeed 
        // Create a 2nd IndexWriter.  This is normally not allowed but it should run through since we're not
        // using any locks:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
        } catch (Exception e) {
            e.printStackTrace(System.out);
            fail("Should not have hit an IOException with no locking");
        }

        writer.close();
        if (writer2 != null) {
            writer2.close();
        }
    }

    static class MockLockFactory extends LockFactory {

        public Map<String,Lock> locksCreated = Collections.synchronizedMap(new HashMap<String,Lock>());

        @Override
        public synchronized Lock obtainLock(Directory dir, String lockName) {
            Lock lock = new MockLock();
            locksCreated.put(lockName, lock);
            return lock;
        }

        public static class MockLock extends Lock {

            @Override
            public void close() {
                // do nothing
            }

            @Override
            public void ensureValid() throws IOException {
              // do nothing
            }

        }
    }

    private void addDoc(IndexWriter writer) throws IOException {
        Document doc = new Document();
        doc.add(newTextField("content", "aaa", Field.Store.NO));
        writer.addDocument(doc);
    }
}
