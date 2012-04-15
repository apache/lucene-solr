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

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene3x.PreFlexRWCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.Assume;

public class TestMixedCodecs extends LuceneTestCase {

  public void test() throws Exception {

    Assume.assumeTrue(!(Codec.getDefault() instanceof PreFlexRWCodec));

    final int NUM_DOCS = atLeast(1000);

    final Directory dir = newDirectory();
    RandomIndexWriter w = null;

    int docsLeftInThisSegment = 0;
    
    int docUpto = 0;
    while (docUpto < NUM_DOCS) {
      if (VERBOSE) {
        System.out.println("TEST: " + docUpto + " of " + NUM_DOCS);
      }
      if (docsLeftInThisSegment == 0) {
        final IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
        if (random().nextBoolean()) {
          // Make sure we aggressively mix in SimpleText
          // since it has different impls for all codec
          // formats...
          iwc.setCodec(Codec.forName("SimpleText"));
        }
        if (w != null) {
          w.close();
        }
        w = new RandomIndexWriter(random(), dir, iwc);
        docsLeftInThisSegment = _TestUtil.nextInt(random(), 10, 100);
      }
      final Document doc = new Document();
      doc.add(newField("id", String.valueOf(docUpto), StringField.TYPE_STORED));
      w.addDocument(doc);
      docUpto++;
      docsLeftInThisSegment--;
    }

    if (VERBOSE) {
      System.out.println("\nTEST: now delete...");
    }

    // Random delete half the docs:
    final Set<Integer> deleted = new HashSet<Integer>();
    while(deleted.size() < NUM_DOCS/2) {
      final Integer toDelete = random().nextInt(NUM_DOCS);
      if (!deleted.contains(toDelete)) {
        deleted.add(toDelete);
        w.deleteDocuments(new Term("id", String.valueOf(toDelete)));
        if (random().nextInt(17) == 6) {
          final IndexReader r = w.getReader();
          assertEquals(NUM_DOCS - deleted.size(), r.numDocs());
          r.close();
        }
      }
    }

    w.close();
    dir.close();
  }
}
