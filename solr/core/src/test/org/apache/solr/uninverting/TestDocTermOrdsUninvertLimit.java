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

package org.apache.solr.uninverting;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.index.SlowCompositeReaderWrapper;

public class TestDocTermOrdsUninvertLimit extends LuceneTestCase {

  /* UnInvertedField had a reference block limitation of 2^24. This unit test triggered it.
   *
   * With the current code, the test verifies that the old limit no longer applies.
   * New limit is 2^31, which is not very realistic to unit-test. */
  @SuppressWarnings({"ConstantConditions", "PointlessBooleanExpression"})
  @Nightly
  // commented 4-Sep-2018   @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testTriggerUnInvertLimit() throws IOException {
    final boolean SHOULD_TRIGGER = false; // Set this to true to use the test with the old implementation

    // Ensure enough terms inside of a single UnInvert-pass-structure to trigger the limit
    final int REF_LIMIT = (int) Math.pow(2, 24); // Maximum number of references within a single pass-structure
    final int DOCS = (1<<16)-1;                  // The number of documents within a single pass (simplified)
    final int TERMS = REF_LIMIT/DOCS;            // Each document must have this many references aka terms hit limit

    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    Field field = newTextField("field", "", Field.Store.NO);
    doc.add(field);

    StringBuilder sb = new StringBuilder(TERMS*(Integer.toString(TERMS).length()+1));
    for (int i = 0 ; i < TERMS ; i++) {
      sb.append(" ").append(Integer.toString(i));
    }
    field.setStringValue(sb.toString());

    for (int i = 0 ; i < DOCS ; i++) {
      w.addDocument(doc);
    }
    //System.out.println("\n Finished adding " + DOCS + " documents of " + TERMS + " unique terms");
    final IndexReader r = w.getReader();
    w.close();

    try {
      final LeafReader ar = SlowCompositeReaderWrapper.wrap(r);
      TestUtil.checkReader(ar);
      final DocTermOrds dto = new DocTermOrds(ar, ar.getLiveDocs(), "field"); // bigTerms turned off
      if (SHOULD_TRIGGER) {
        fail("DocTermOrds should have failed with a \"Too many values for UnInvertedField\" message");
      }
    } catch (IllegalStateException e) {
      if (!SHOULD_TRIGGER) {
        fail("DocTermsOrd should not have failed with this implementation, but got exception " +
            e.getClass().getSimpleName() + " with message " + e.getMessage());
      }
      // This is (hopefully) "Too many values for UnInvertedField faceting on field field", so all is as expected
    } finally {
      r.close();
      dir.close();
    }
  }
}
