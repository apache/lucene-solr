package org.apache.lucene.index;

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
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.index.IndexWriter;

/**
 * This tests the patch for issue #LUCENE-715 (IndexWriter does not
 * release its write lock when trying to open an index which does not yet
 * exist).
 *
 * @version $Id$
 */

public class TestIndexWriterLockRelease extends LuceneTestCase {
    private java.io.File __test_dir;

    public void setUp() throws Exception {
        super.setUp();
        if (this.__test_dir == null) {
            String tmp_dir = System.getProperty("java.io.tmpdir", "tmp");
            this.__test_dir = new File(tmp_dir, "testIndexWriter");

            if (this.__test_dir.exists()) {
                throw new IOException("test directory \"" + this.__test_dir.getPath() + "\" already exists (please remove by hand)");
            }

            if (!this.__test_dir.mkdirs()
                && !this.__test_dir.isDirectory()) {
                throw new IOException("unable to create test directory \"" + this.__test_dir.getPath() + "\"");
            }
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if (this.__test_dir != null) {
            File[] files = this.__test_dir.listFiles();

            for (int i = 0;
                i < files.length;
                ++i) {
                if (!files[i].delete()) {
                    throw new IOException("unable to remove file in test directory \"" + this.__test_dir.getPath() + "\" (please remove by hand)");
                }
            }

            if (!this.__test_dir.delete()) {
                throw new IOException("unable to remove test directory \"" + this.__test_dir.getPath() + "\" (please remove by hand)");
            }
        }
    }

    public void testIndexWriterLockRelease() throws IOException {
        IndexWriter im;

        try {
            im = new IndexWriter(this.__test_dir, new org.apache.lucene.analysis.standard.StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
        } catch (FileNotFoundException e) {
            try {
                im = new IndexWriter(this.__test_dir, new org.apache.lucene.analysis.standard.StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
            } catch (FileNotFoundException e1) {
            }
        }
    }
}
