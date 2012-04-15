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
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;

/**
 * This tests the patch for issue #LUCENE-715 (IndexWriter does not
 * release its write lock when trying to open an index which does not yet
 * exist).
 */

public class TestIndexWriterLockRelease extends LuceneTestCase {
    private java.io.File __test_dir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (this.__test_dir == null) {
            this.__test_dir = _TestUtil.getTempDir("testIndexWriter");

            if (this.__test_dir.exists()) {
                throw new IOException("test directory \"" + this.__test_dir.getPath() + "\" already exists (please remove by hand)");
            }

            if (!this.__test_dir.mkdirs()
                && !this.__test_dir.isDirectory()) {
                throw new IOException("unable to create test directory \"" + this.__test_dir.getPath() + "\"");
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
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
        super.tearDown();
    }

    public void testIndexWriterLockRelease() throws IOException {
      Directory dir = newFSDirectory(this.__test_dir);
      try {
        new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
      } catch (FileNotFoundException e) {
        try {
          new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
        } catch (FileNotFoundException e1) {
        }
      } finally {
        dir.close();
      }
    }
}
