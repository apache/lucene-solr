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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;

public class TestMultiFields extends LuceneTestCase {

  public void testRandom() throws Exception {

    int num = atLeast(2);
    for (int iter = 0; iter < num; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }

      Directory dir = newDirectory();

      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                             .setMergePolicy(NoMergePolicy.INSTANCE));
      // we can do this because we use NoMergePolicy (and dont merge to "nothing")
      w.setKeepFullyDeletedSegments(true);

      Map<BytesRef,List<Integer>> docs = new HashMap<>();
      Set<Integer> deleted = new HashSet<>();
      List<BytesRef> terms = new ArrayList<>();

      int numDocs = TestUtil.nextInt(random(), 1, 100 * RANDOM_MULTIPLIER);
      Document doc = new Document();
      Field f = newStringField("field", "", Field.Store.NO);
      doc.add(f);
      Field id = newStringField("id", "", Field.Store.NO);
      doc.add(id);

      boolean onlyUniqueTerms = random().nextBoolean();
      if (VERBOSE) {
        System.out.println("TEST: onlyUniqueTerms=" + onlyUniqueTerms + " numDocs=" + numDocs);
      }
      Set<BytesRef> uniqueTerms = new HashSet<>();
      for(int i=0;i<numDocs;i++) {

        if (!onlyUniqueTerms && random().nextBoolean() && terms.size() > 0) {
          // re-use existing term
          BytesRef term = terms.get(random().nextInt(terms.size()));
          docs.get(term).add(i);
          f.setStringValue(term.utf8ToString());
        } else {
          String s = TestUtil.randomUnicodeString(random(), 10);
          BytesRef term = new BytesRef(s);
          if (!docs.containsKey(term)) {
            docs.put(term, new ArrayList<Integer>());
          }
          docs.get(term).add(i);
          terms.add(term);
          uniqueTerms.add(term);
          f.setStringValue(s);
        }
        id.setStringValue(""+i);
        w.addDocument(doc);
        if (random().nextInt(4) == 1) {
          w.commit();
        }
        if (i > 0 && random().nextInt(20) == 1) {
          int delID = random().nextInt(i);
          deleted.add(delID);
          w.deleteDocuments(new Term("id", ""+delID));
          if (VERBOSE) {
            System.out.println("TEST: delete " + delID);
          }
        }
      }

      if (VERBOSE) {
        List<BytesRef> termsList = new ArrayList<>(uniqueTerms);
        Collections.sort(termsList, BytesRef.getUTF8SortedAsUTF16Comparator());
        System.out.println("TEST: terms in UTF16 order:");
        for(BytesRef b : termsList) {
          System.out.println("  " + UnicodeUtil.toHexString(b.utf8ToString()) + " " + b);
          for(int docID : docs.get(b)) {
            if (deleted.contains(docID)) {
              System.out.println("    " + docID + " (deleted)");
            } else {
              System.out.println("    " + docID);
            }
          }
        }
      }

      IndexReader reader = w.getReader();
      w.close();
      if (VERBOSE) {
        System.out.println("TEST: reader=" + reader);
      }

      Bits liveDocs = MultiFields.getLiveDocs(reader);
      for(int delDoc : deleted) {
        assertFalse(liveDocs.get(delDoc));
      }

      for(int i=0;i<100;i++) {
        BytesRef term = terms.get(random().nextInt(terms.size()));
        if (VERBOSE) {
          System.out.println("TEST: seek term="+ UnicodeUtil.toHexString(term.utf8ToString()) + " " + term);
        }
        
        PostingsEnum postingsEnum = TestUtil.docs(random(), reader, "field", term, null, PostingsEnum.NONE);
        assertNotNull(postingsEnum);

        for(int docID : docs.get(term)) {
          assertEquals(docID, postingsEnum.nextDoc());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, postingsEnum.nextDoc());
      }

      reader.close();
      dir.close();
    }
  }

  /*
  private void verify(IndexReader r, String term, List<Integer> expected) throws Exception {
    DocsEnum docs = _TestUtil.docs(random, r,
                                   "field",
                                   new BytesRef(term),
                                   MultiFields.getLiveDocs(r),
                                   null,
                                   false);
    for(int docID : expected) {
      assertEquals(docID, docs.nextDoc());
    }
    assertEquals(docs.NO_MORE_DOCS, docs.nextDoc());
  }
  */

  public void testSeparateEnums() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document d = new Document();
    d.add(newStringField("f", "j", Field.Store.NO));
    w.addDocument(d);
    w.commit();
    w.addDocument(d);
    IndexReader r = w.getReader();
    w.close();
    PostingsEnum d1 = TestUtil.docs(random(), r, "f", new BytesRef("j"), null, PostingsEnum.NONE);
    PostingsEnum d2 = TestUtil.docs(random(), r, "f", new BytesRef("j"), null, PostingsEnum.NONE);
    assertEquals(0, d1.nextDoc());
    assertEquals(0, d2.nextDoc());
    r.close();
    dir.close();
  }
  
  public void testTermDocsEnum() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document d = new Document();
    d.add(newStringField("f", "j", Field.Store.NO));
    w.addDocument(d);
    w.commit();
    w.addDocument(d);
    IndexReader r = w.getReader();
    w.close();
    PostingsEnum de = MultiFields.getTermDocsEnum(r, "f", new BytesRef("j"));
    assertEquals(0, de.nextDoc());
    assertEquals(1, de.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, de.nextDoc());
    r.close();
    dir.close();
  }
}
