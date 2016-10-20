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
package org.apache.lucene.search;

import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;

import org.junit.BeforeClass;
import org.junit.Assume;


/**
 * subclass of TestSimpleExplanations that adds a lot of filler docs which will be ignored at query time.
 * These filler docs will either all be empty in which case the queries will be unmodified, or they will 
 * all use terms from same set of source data as our regular docs (to emphasis the DocFreq factor in scoring), 
 * in which case the queries will be wrapped so they can be excluded.
 */
@Slow // can this be sped up to be non-slow? filler docs make it quite a bit slower and many test methods...
public class TestSimpleExplanationsWithFillerDocs extends TestSimpleExplanations {

  /** num of empty docs injected between every doc in the index */
  private static final int NUM_FILLER_DOCS = BooleanScorer.SIZE;
  /** num of empty docs injected prior to the first doc in the (main) index */
  private static int PRE_FILLER_DOCS;
  /** 
   * If non-null then the filler docs are not empty, and need to be filtered out from queries 
   * using this as both field name &amp; field value 
   */
  public static String EXTRA = null;

  private static final Document EMPTY_DOC = new Document();
  
  /**
   * Replaces the index created by our superclass with a new one that includes a lot of docs filler docs.
   * {@link #qtest} will account for these extra filler docs.
   * @see #qtest
   */
  @BeforeClass
  public static void replaceIndex() throws Exception {
    EXTRA = random().nextBoolean() ? null : "extra";
    PRE_FILLER_DOCS = TestUtil.nextInt(random(), 0, (NUM_FILLER_DOCS / 2));

    // free up what our super class created that we won't be using
    reader.close();
    directory.close();
    
    directory = newDirectory();
    try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(analyzer).setMergePolicy(newLogMergePolicy()))) {

      for (int filler = 0; filler < PRE_FILLER_DOCS; filler++) {
        writer.addDocument(makeFillerDoc());
      }
      for (int i = 0; i < docFields.length; i++) {
        writer.addDocument(createDoc(i));
        
        for (int filler = 0; filler < NUM_FILLER_DOCS; filler++) {
          writer.addDocument(makeFillerDoc());
        }
      }
      reader = writer.getReader();
      searcher = newSearcher(reader);
    }
  }

  private static Document makeFillerDoc() {
    if (null == EXTRA) {
      return EMPTY_DOC;
    }
    Document doc = createDoc(TestUtil.nextInt(random(), 0, docFields.length-1));
    doc.add(newStringField(EXTRA, EXTRA, Field.Store.NO));
    return doc;
  }

  /**
   * Adjusts <code>expDocNrs</code> based on the filler docs injected in the index, 
   * and if neccessary wraps the <code>q</code> in a BooleanQuery that will filter out all 
   * filler docs using the {@link #EXTRA} field.
   * 
   * @see #replaceIndex
   */
  @Override
  public void qtest(Query q, int[] expDocNrs) throws Exception {

    expDocNrs = Arrays.copyOf(expDocNrs, expDocNrs.length);
    for (int i=0; i < expDocNrs.length; i++) {
      expDocNrs[i] = PRE_FILLER_DOCS + ((NUM_FILLER_DOCS + 1) * expDocNrs[i]);
    }

    if (null != EXTRA) {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(new BooleanClause(q, BooleanClause.Occur.MUST));
      builder.add(new BooleanClause(new TermQuery(new Term(EXTRA, EXTRA)), BooleanClause.Occur.MUST_NOT));
      q = builder.build();
    }
    super.qtest(q, expDocNrs);
  }

  public void testMA1() throws Exception {
    Assume.assumeNotNull("test is not viable with empty filler docs", EXTRA);
    super.testMA1();
  }
  public void testMA2() throws Exception {
    Assume.assumeNotNull("test is not viable with empty filler docs", EXTRA);
    super.testMA2();
  }

  
}
