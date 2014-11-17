package org.apache.lucene;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;


/* Intentionally outside of oal.index to verify fully
   external codecs work fine */

public class TestExternalCodecs extends LuceneTestCase {

  // tests storing "id" and "field2" fields as pulsing codec,
  // whose term sort is backwards unicode code point, and
  // storing "field1" as a custom entirely-in-RAM codec
  public void testPerFieldCodec() throws Exception {
    
    final int NUM_DOCS = atLeast(173);
    if (VERBOSE) {
      System.out.println("TEST: NUM_DOCS=" + NUM_DOCS);
    }

    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // we use a custom codec provider
    IndexWriter w = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy(3))
    );

    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setPostingsFormat("id", "RAMOnly");
    fieldTypes.disableFastRanges("id");
    fieldTypes.setPostingsFormat("field1", TestUtil.getDefaultPostingsFormat().getName());
    fieldTypes.setPostingsFormat("field2", "Memory");
    
    for(int i=0;i<NUM_DOCS;i++) {
      Document2 doc = w.newDocument();
      // uses default codec:
      doc.addLargeText("field1", "this field uses the standard codec as the test");
      // uses memory codec:
      doc.addLargeText("field2", "this field uses the memory codec as the test");
      doc.addUniqueInt("id", i);
      w.addDocument(doc);
      if ((i+1)%10 == 0) {
        w.commit();
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: now delete id=77");
    }
    w.deleteDocuments(fieldTypes.newIntTerm("id", 77));

    IndexReader r = DirectoryReader.open(w, true);
    
    assertEquals(NUM_DOCS-1, r.numDocs());
    IndexSearcher s = newSearcher(r);
    assertEquals(NUM_DOCS-1, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(NUM_DOCS-1, s.search(new TermQuery(new Term("field2", "memory")), 1).totalHits);
    r.close();

    if (VERBOSE) {
      System.out.println("\nTEST: now delete 2nd doc");
    }
    w.deleteDocuments(fieldTypes.newIntTerm("id", 44));

    if (VERBOSE) {
      System.out.println("\nTEST: now force merge");
    }
    w.forceMerge(1);
    if (VERBOSE) {
      System.out.println("\nTEST: now open reader");
    }
    r = DirectoryReader.open(w, true);
    assertEquals(NUM_DOCS-2, r.maxDoc());
    assertEquals(NUM_DOCS-2, r.numDocs());
    s = newSearcher(r);
    assertEquals(NUM_DOCS-2, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(NUM_DOCS-2, s.search(new TermQuery(new Term("field2", "memory")), 1).totalHits);
    assertEquals(1, s.search(fieldTypes.newIntTermQuery("id", 76), 1).totalHits);
    assertEquals(0, s.search(fieldTypes.newIntTermQuery("id", 77), 1).totalHits);
    assertEquals(0, s.search(fieldTypes.newIntTermQuery("id", 44), 1).totalHits);

    if (VERBOSE) {
      System.out.println("\nTEST: now close NRT reader");
    }
    r.close();

    w.close();

    dir.close();
  }
}
