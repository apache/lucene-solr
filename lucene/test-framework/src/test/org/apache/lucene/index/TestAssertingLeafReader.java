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
package org.apache.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.QueryUtils.FCInvisibleMultiReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestAssertingLeafReader extends LuceneTestCase {
  public void testAssertBits() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    // Not deleted:
    w.addDocument(new Document());

    // Does get deleted:
    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.NO));
    w.addDocument(doc);
    w.commit();

    w.deleteDocuments(new Term("id", "0"));
    w.close();

    // Now we have index with 1 segment with 2 docs one of which is marked deleted

    IndexReader r = DirectoryReader.open(dir);
    assertEquals(1, r.leaves().size());
    assertEquals(2, r.maxDoc());
    assertEquals(1, r.numDocs());

    r = new AssertingDirectoryReader((DirectoryReader) r);

    final IndexReader r2 = SlowCompositeReaderWrapper.wrap(r);
   
    Thread thread = new Thread() {
      @Override
      public void run() {
        for(LeafReaderContext context : r2.leaves()) {
          context.reader().getLiveDocs().get(0);
        }
      }
    };
    thread.start();
    thread.join();

    IOUtils.close(r2, dir);
  }
}
