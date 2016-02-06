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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;

public class TestStressAdvance extends LuceneTestCase {

  public void testStressAdvance() throws Exception {
    for(int iter=0;iter<3;iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }
      Directory dir = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      final Set<Integer> aDocs = new HashSet<>();
      final Document doc = new Document();
      final Field f = newStringField("field", "", Field.Store.NO);
      doc.add(f);
      final Field idField = newStringField("id", "", Field.Store.YES);
      doc.add(idField);
      int num = atLeast(4097);
      if (VERBOSE) {
        System.out.println("\nTEST: numDocs=" + num);
      }
      for(int id=0;id<num;id++) {
        if (random().nextInt(4) == 3) {
          f.setStringValue("a");
          aDocs.add(id);
        } else {
          f.setStringValue("b");
        }
        idField.setStringValue(""+id);
        w.addDocument(doc);
        if (VERBOSE) {
          System.out.println("\nTEST: doc upto " + id);
        }
      }

      w.forceMerge(1);

      final List<Integer> aDocIDs = new ArrayList<>();
      final List<Integer> bDocIDs = new ArrayList<>();

      final DirectoryReader r = w.getReader();
      final int[] idToDocID = new int[r.maxDoc()];
      for(int docID=0;docID<idToDocID.length;docID++) {
        int id = Integer.parseInt(r.document(docID).get("id"));
        if (aDocs.contains(id)) {
          aDocIDs.add(docID);
        } else {
          bDocIDs.add(docID);
        }
      }
      final TermsEnum te = getOnlySegmentReader(r).fields().terms("field").iterator();
      
      PostingsEnum de = null;
      for(int iter2=0;iter2<10;iter2++) {
        if (VERBOSE) {
          System.out.println("\nTEST: iter=" + iter + " iter2=" + iter2);
        }
        assertEquals(TermsEnum.SeekStatus.FOUND, te.seekCeil(new BytesRef("a")));
        de = TestUtil.docs(random(), te, de, PostingsEnum.NONE);
        testOne(de, aDocIDs);

        assertEquals(TermsEnum.SeekStatus.FOUND, te.seekCeil(new BytesRef("b")));
        de = TestUtil.docs(random(), te, de, PostingsEnum.NONE);
        testOne(de, bDocIDs);
      }

      w.close();
      r.close();
      dir.close();
    }
  }

  private void testOne(PostingsEnum docs, List<Integer> expected) throws Exception {
    if (VERBOSE) {
      System.out.println("test");
    }
    int upto = -1;
    while(upto < expected.size()) {
      if (VERBOSE) {
        System.out.println("  cycle upto=" + upto + " of " + expected.size());
      }
      final int docID;
      if (random().nextInt(4) == 1 || upto == expected.size()-1) {
        // test nextDoc()
        if (VERBOSE) {
          System.out.println("    do nextDoc");
        }
        upto++;
        docID = docs.nextDoc();
      } else {
        // test advance()
        final int inc = TestUtil.nextInt(random(), 1, expected.size() - 1 - upto);
        if (VERBOSE) {
          System.out.println("    do advance inc=" + inc);
        }
        upto += inc;
        docID = docs.advance(expected.get(upto));
      }
      if (upto == expected.size()) {
        if (VERBOSE) {
          System.out.println("  expect docID=" + DocIdSetIterator.NO_MORE_DOCS + " actual=" + docID);
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docID);
      } else {
        if (VERBOSE) {
          System.out.println("  expect docID=" + expected.get(upto) + " actual=" + docID);
        }
        assertTrue(docID != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals(expected.get(upto).intValue(), docID);
      }
    }
  }
}
