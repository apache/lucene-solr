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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestTermsEnum extends LuceneTestCase {

  public void test() throws Exception {
    final LineFileDocs docs = new LineFileDocs(random);
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, d);
    final int numDocs = atLeast(10);
    for(int docCount=0;docCount<numDocs;docCount++) {
      w.addDocument(docs.nextDoc());
    }
    final IndexReader r = w.getReader();
    w.close();

    final List<BytesRef> terms = new ArrayList<BytesRef>();
    final TermsEnum termsEnum = MultiFields.getTerms(r, "body").iterator();
    BytesRef term;
    while((term = termsEnum.next()) != null) {
      terms.add(new BytesRef(term));
    }
    if (VERBOSE) {
      System.out.println("TEST: " + terms.size() + " terms");
    }

    int upto = -1;
    final int iters = atLeast(200);
    for(int iter=0;iter<iters;iter++) {
      final boolean isEnd;
      if (upto != -1 && random.nextBoolean()) {
        // next
        if (VERBOSE) {
          System.out.println("TEST: iter next");
        }
        isEnd = termsEnum.next() == null;
        upto++;
        if (isEnd) {
          if (VERBOSE) {
            System.out.println("  end");
          }
          assertEquals(upto, terms.size());
          upto = -1;
        } else {
          if (VERBOSE) {
            System.out.println("  got term=" + termsEnum.term().utf8ToString() + " expected=" + terms.get(upto).utf8ToString());
          }
          assertTrue(upto < terms.size());
          assertEquals(terms.get(upto), termsEnum.term());
        }
      } else {

        final BytesRef target;
        final String exists;
        if (random.nextBoolean()) {
          // likely fake term
          if (random.nextBoolean()) {
            target = new BytesRef(_TestUtil.randomSimpleString(random));
          } else {
            target = new BytesRef(_TestUtil.randomRealisticUnicodeString(random));
          }
          exists = "likely not";
        } else {
          // real term
          target = terms.get(random.nextInt(terms.size()));
          exists = "yes";
        }

        upto = Collections.binarySearch(terms, target);

        if (random.nextBoolean()) {
          if (VERBOSE) {
            System.out.println("TEST: iter seekCeil target=" + target.utf8ToString() + " exists=" + exists);
          }
          // seekCeil
          final TermsEnum.SeekStatus status = termsEnum.seekCeil(target, random.nextBoolean());
          if (VERBOSE) {
            System.out.println("  got " + status);
          }
          
          if (upto < 0) {
            upto = -(upto+1);
            if (upto >= terms.size()) {
              assertEquals(TermsEnum.SeekStatus.END, status);
              upto = -1;
            } else {
              assertEquals(TermsEnum.SeekStatus.NOT_FOUND, status);
              assertEquals(terms.get(upto), termsEnum.term());
            }
          } else {
            assertEquals(TermsEnum.SeekStatus.FOUND, status);
            assertEquals(terms.get(upto), termsEnum.term());
          }
        } else {
          if (VERBOSE) {
            System.out.println("TEST: iter seekExact target=" + target.utf8ToString() + " exists=" + exists);
          }
          // seekExact
          final boolean result = termsEnum.seekExact(target, false);
          if (VERBOSE) {
            System.out.println("  got " + result);
          }
          if (upto < 0) {
            assertFalse(result);
            upto = -1;
          } else {
            assertTrue(result);
            assertEquals(target, termsEnum.term());
          }
        }
      }
    }

    r.close();
    d.close();
  }
}
