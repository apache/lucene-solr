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
package org.apache.lucene.search.spans;


import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.lucene.search.spans.SpanTestUtil.*;

/**
 * Tests Spans (v2)
 *
 */
public class TestSpansEnum extends LuceneTestCase {
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000)).setMergePolicy(newLogMergePolicy()));
    //writer.infoStream = System.out;
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(newTextField("field", English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    for (int i = 100; i < 110; i++) {
      Document doc = new Document(); // doc id 10-19 have 100-109
      doc.add(newTextField("field", English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    searcher = null;
    reader = null;
    directory = null;
  }

  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, "field", searcher, results);
  }

  public void testSpansEnumOr1() throws Exception {
    checkHits(spanOrQuery("field", "one", "two"), 
              new int[] {1, 2, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  }

  public void testSpansEnumOr2() throws Exception {
    checkHits(spanOrQuery("field", "one", "eleven"), 
              new int[] {1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  }

  public void testSpansEnumOr3() throws Exception {
    checkHits(spanOrQuery("field", "twelve", "eleven"), 
              new int[] {});
  }
  
  public SpanQuery spanTQ(String s) {
    return spanTermQuery("field", s);
  }

  public void testSpansEnumOrNot1() throws Exception {
    checkHits(spanNotQuery(spanOrQuery("field", "one", "two"), spanTermQuery("field", "one")),
              new int[] {2,12});
  }

  public void testSpansEnumNotBeforeAfter1() throws Exception {
    checkHits(spanNotQuery(spanTermQuery("field", "hundred"), spanTermQuery("field", "one")), 
              new int[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19}); // include all "one hundred ..."
  }

  public void testSpansEnumNotBeforeAfter2() throws Exception {
    checkHits(spanNotQuery(spanTermQuery("field", "hundred"), spanTermQuery("field", "one"), 1, 0),
              new int[] {}); // exclude all "one hundred ..."
  }

  public void testSpansEnumNotBeforeAfter3() throws Exception {
    checkHits(spanNotQuery(spanTermQuery("field", "hundred"), spanTermQuery("field", "one"), 0, 1),
              new int[] {10, 12, 13, 14, 15, 16, 17, 18, 19}); // exclude "one hundred one"
  }
}
