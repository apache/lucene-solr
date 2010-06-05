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

    for(int iter=0;iter<2*_TestUtil.getRandomMultiplier();iter++) {
      Directory dir = new MockRAMDirectory();
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()).setMergePolicy(NoMergePolicy.COMPOUND_FILES));

      Random r = new Random();

      Map<BytesRef,List<Integer>> docs = new HashMap<BytesRef,List<Integer>>();
      Set<Integer> deleted = new HashSet<Integer>();
      List<BytesRef> terms = new ArrayList<BytesRef>();

      int numDocs = r.nextInt(100*_TestUtil.getRandomMultiplier());
      Document doc = new Document();
      Field f = new Field("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
      doc.add(f);
      Field id = new Field("id", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
      doc.add(id);

      boolean onlyUniqueTerms = r.nextBoolean();

      for(int i=0;i<numDocs;i++) {

        if (!onlyUniqueTerms && r.nextBoolean() && terms.size() > 0) {
          // re-use existing term
          BytesRef term = terms.get(r.nextInt(terms.size()));
          docs.get(term).add(i);
          f.setValue(term.utf8ToString());
        } else {
          String s = _TestUtil.randomUnicodeString(r, 10);
          BytesRef term = new BytesRef(s);
          if (!docs.containsKey(term)) {
            docs.put(term, new ArrayList<Integer>());
          }
          docs.get(term).add(i);
          terms.add(term);
          f.setValue(s);
        }
        id.setValue(""+i);
        w.addDocument(doc);
        if (r.nextInt(4) == 1) {
          w.commit();
        }
        if (i > 0 && r.nextInt(20) == 1) {
          int delID = r.nextInt(i);
          deleted.add(delID);
          w.deleteDocuments(new Term("id", ""+delID));
        }
      }

      IndexReader reader = w.getReader();
      w.close();

      Bits delDocs = MultiFields.getDeletedDocs(reader);
      for(int delDoc : deleted) {
        assertTrue(delDocs.get(delDoc));
      }
      Terms terms2 = MultiFields.getTerms(reader, "field");

      for(int i=0;i<100;i++) {
        BytesRef term = terms.get(r.nextInt(terms.size()));
        
        DocsEnum docsEnum = terms2.docs(delDocs, term, null);
        int count = 0;
        for(int docID : docs.get(term)) {
          if (!deleted.contains(docID)) {
            assertEquals(docID, docsEnum.nextDoc());
            count++;
          }
        }
        //System.out.println("c=" + count + " t=" + term);
        assertEquals(docsEnum.NO_MORE_DOCS, docsEnum.nextDoc());
      }

      reader.close();
      dir.close();
    }
  }

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
}
