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
package org.apache.lucene;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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

  private static final class CustomPerFieldCodec extends AssertingCodec {
    
    private final PostingsFormat ramFormat = PostingsFormat.forName("RAMOnly");
    private final PostingsFormat defaultFormat = TestUtil.getDefaultPostingsFormat();

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      if (field.equals("field2") || field.equals("field1") || field.equals("id")) {
        return defaultFormat;
      } else {
        return ramFormat;
      }
    }
  }

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
        newIndexWriterConfig(new MockAnalyzer(random())).
        setCodec(new CustomPerFieldCodec()).
            setMergePolicy(newLogMergePolicy(3))
    );
    Document doc = new Document();
    // uses default codec:
    doc.add(newTextField("field1", "this field uses the standard codec as the test", Field.Store.NO));
    // uses memory codec:
    Field field2 = newTextField("field2", "this field uses the memory codec as the test", Field.Store.NO);
    doc.add(field2);
    
    Field idField = newStringField("id", "", Field.Store.NO);

    doc.add(idField);
    for(int i=0;i<NUM_DOCS;i++) {
      idField.setStringValue(""+i);
      w.addDocument(doc);
      if ((i+1)%10 == 0) {
        w.commit();
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: now delete id=77");
    }
    w.deleteDocuments(new Term("id", "77"));

    IndexReader r = DirectoryReader.open(w);
    
    assertEquals(NUM_DOCS-1, r.numDocs());
    IndexSearcher s = newSearcher(r);
    assertEquals(NUM_DOCS-1, s.count(new TermQuery(new Term("field1", "standard"))));
    assertEquals(NUM_DOCS-1, s.count(new TermQuery(new Term("field2", "memory"))));
    r.close();

    if (VERBOSE) {
      System.out.println("\nTEST: now delete 2nd doc");
    }
    w.deleteDocuments(new Term("id", "44"));

    if (VERBOSE) {
      System.out.println("\nTEST: now force merge");
    }
    w.forceMerge(1);
    if (VERBOSE) {
      System.out.println("\nTEST: now open reader");
    }
    r = DirectoryReader.open(w);
    assertEquals(NUM_DOCS-2, r.maxDoc());
    assertEquals(NUM_DOCS-2, r.numDocs());
    s = newSearcher(r);
    assertEquals(NUM_DOCS-2, s.count(new TermQuery(new Term("field1", "standard"))));
    assertEquals(NUM_DOCS-2, s.count(new TermQuery(new Term("field2", "memory"))));
    assertEquals(1, s.count(new TermQuery(new Term("id", "76"))));
    assertEquals(0, s.count(new TermQuery(new Term("id", "77"))));
    assertEquals(0, s.count(new TermQuery(new Term("id", "44"))));

    if (VERBOSE) {
      System.out.println("\nTEST: now close NRT reader");
    }
    r.close();

    w.close();

    dir.close();
  }
}
