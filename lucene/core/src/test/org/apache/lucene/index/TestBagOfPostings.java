package org.apache.lucene.index;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;

/**
 * Simple test that adds numeric terms, where each term has the 
 * docFreq of its integer value, and checks that the docFreq is correct. 
 */
@SuppressCodecs({"Direct", "Memory"}) // at night this makes like 200k/300k docs and will make Direct's heart beat!
public class TestBagOfPostings extends LuceneTestCase {
  public void test() throws Exception {
    LinkedList<String> postings = new LinkedList<String>();
    int numTerms = atLeast(300);
    int maxTermsPerDoc = 10;
    for (int i = 0; i < numTerms; i++) {
      String term = Integer.toString(i);
      for (int j = 0; j < i; j++) {
        postings.add(term);
      }
    }
    Collections.shuffle(postings, random());
    
    Directory dir = newFSDirectory(_TestUtil.getTempDir("bagofpostings"));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document document = new Document();
    Field field = newTextField("field", "", Field.Store.NO);
    document.add(field);
    
    while (!postings.isEmpty()) {
      StringBuilder text = new StringBuilder();
      Set<String> visited = new HashSet<String>();
      for (int i = 0; i < maxTermsPerDoc; i++) {
        if (postings.isEmpty() || visited.contains(postings.peek())) {
          break;
        }
        String element = postings.remove();
        text.append(' ');
        text.append(element);
        visited.add(element);
      }
      field.setStringValue(text.toString());
      iw.addDocument(document);
    }
    iw.forceMerge(1);
    DirectoryReader ir = iw.getReader();
    assertEquals(1, ir.leaves().size());
    AtomicReader air = ir.leaves().get(0).reader();
    Terms terms = air.terms("field");
    TermsEnum termsEnum = terms.iterator(null);
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      int value = Integer.parseInt(term.utf8ToString());
      assertEquals(value, termsEnum.docFreq());
      // don't really need to check more than this, as CheckIndex
      // will verify that docFreq == actual number of documents seen
      // from a docsAndPositionsEnum.
    }
    ir.close();
    iw.close();
    dir.close();
  }
}
