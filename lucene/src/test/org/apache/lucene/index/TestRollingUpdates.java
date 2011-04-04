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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.junit.Test;

public class TestRollingUpdates extends LuceneTestCase {

  // Just updates the same set of N docs over and over, to
  // stress out deletions

  @Test
  public void testRollingUpdates() throws Exception {
    final Directory dir = newDirectory();

    final LineFileDocs docs = new LineFileDocs(random);

    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()));
    final int SIZE = 200 * RANDOM_MULTIPLIER;
    int id = 0;
    IndexReader r = null;
    final int numUpdates = (int) (SIZE * (2+random.nextDouble()));
    for(int docIter=0;docIter<numUpdates;docIter++) {
      final Document doc = docs.nextDoc();
      final String myID = ""+id;
      if (id == SIZE-1) {
        id = 0;
      } else {
        id++;
      }
      doc.getField("docid").setValue(myID);
      w.updateDocument(new Term("docid", myID), doc);

      if (docIter >= SIZE && random.nextInt(50) == 17) {
        if (r != null) {
          r.close();
        }
        final boolean applyDeletions = random.nextBoolean();
        r = w.getReader(applyDeletions);
        assertTrue("applyDeletions=" + applyDeletions + " r.numDocs()=" + r.numDocs() + " vs SIZE=" + SIZE, !applyDeletions || r.numDocs() == SIZE);
      }
    }

    if (r != null) {
      r.close();
    }

    w.commit();
    assertEquals(SIZE, w.numDocs());

    w.close();
    docs.close();
    
    dir.close();
  }
}
