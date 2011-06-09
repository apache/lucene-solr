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

import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import java.util.*;

public class TestMultiFields extends LuceneTestCase {

  public void testRandom() throws Exception {

    int num = atLeast(2);
    for (int iter = 0; iter < num; iter++) {
      Directory dir = newDirectory();

      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.COMPOUND_FILES));
      _TestUtil.keepFullyDeletedSegments(w);

      Map<BytesRef,List<Integer>> docs = new HashMap<BytesRef,List<Integer>>();
      Set<Integer> deleted = new HashSet<Integer>();
      List<BytesRef> terms = new ArrayList<BytesRef>();

      int numDocs = _TestUtil.nextInt(random, 1, 100 * RANDOM_MULTIPLIER);
      Document doc = new Document();
      Field f = newField("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
      doc.add(f);
      Field id = newField("id", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
      doc.add(id);

      boolean onlyUniqueTerms = random.nextBoolean();
      Set<BytesRef> uniqueTerms = new HashSet<BytesRef>();
      for(int i=0;i<numDocs;i++) {

        if (!onlyUniqueTerms && random.nextBoolean() && terms.size() > 0) {
          // re-use existing term
          BytesRef term = terms.get(random.nextInt(terms.size()));
          docs.get(term).add(i);
          f.setValue(term.utf8ToString());
        } else {
          String s = _TestUtil.randomUnicodeString(random, 10);
          BytesRef term = new BytesRef(s);
          if (!docs.containsKey(term)) {
            docs.put(term, new ArrayList<Integer>());
          }
          docs.get(term).add(i);
          terms.add(term);
          uniqueTerms.add(term);
          f.setValue(s);
        }
        id.setValue(""+i);
        w.addDocument(doc);
        if (random.nextInt(4) == 1) {
          w.commit();
        }
        if (i > 0 && random.nextInt(20) == 1) {
          int delID = random.nextInt(i);
          deleted.add(delID);
          w.deleteDocuments(new Term("id", ""+delID));
        }
      }

      if (VERBOSE) {
        List<BytesRef> termsList = new ArrayList<BytesRef>(uniqueTerms);
        Collections.sort(termsList, BytesRef.getUTF8SortedAsUTF16Comparator());
        System.out.println("UTF16 order:");
        for(BytesRef b : termsList) {
          System.out.println("  " + UnicodeUtil.toHexString(b.utf8ToString()));
        }
      }

      IndexReader reader = w.getReader();
      w.close();
      //System.out.println("TEST reader=" + reader);

      Bits delDocs = MultiFields.getDeletedDocs(reader);
      for(int delDoc : deleted) {
        assertTrue(delDocs.get(delDoc));
      }
      Terms terms2 = MultiFields.getTerms(reader, "field");

      for(int i=0;i<100;i++) {
        BytesRef term = terms.get(random.nextInt(terms.size()));
        if (VERBOSE) {
          System.out.println("TEST: seek to term= "+ UnicodeUtil.toHexString(term.utf8ToString()));
        }
        
        DocsEnum docsEnum = terms2.docs(delDocs, term, null);
        assertNotNull(docsEnum);

        for(int docID : docs.get(term)) {
          if (!deleted.contains(docID)) {
            assertEquals(docID, docsEnum.nextDoc());
          }
        }
        assertEquals(docsEnum.NO_MORE_DOCS, docsEnum.nextDoc());
      }

      reader.close();
      dir.close();
    }
  }

  /*
  private void verify(IndexReader r, String term, List<Integer> expected) throws Exception {
    DocsEnum docs = MultiFields.getTermDocsEnum(r,
                                                MultiFields.getDeletedDocs(r),
                                                "field",
                                                new BytesRef(term));

    for(int docID : expected) {
      assertEquals(docID, docs.nextDoc());
    }
    assertEquals(docs.NO_MORE_DOCS, docs.nextDoc());
  }
  */

  public void testSeparateEnums() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d = new Document();
    d.add(newField("f", "j", Field.Store.NO, Field.Index.NOT_ANALYZED));
    w.addDocument(d);
    w.commit();
    w.addDocument(d);
    IndexReader r = w.getReader();
    w.close();
    DocsEnum d1 = MultiFields.getTermDocsEnum(r, null, "f", new BytesRef("j"));
    DocsEnum d2 = MultiFields.getTermDocsEnum(r, null, "f", new BytesRef("j"));
    assertEquals(0, d1.nextDoc());
    assertEquals(0, d2.nextDoc());
    r.close();
    dir.close();
  }
}
