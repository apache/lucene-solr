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

import java.io.IOException;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TimeUnits;
import org.junit.Test;

/**
 * Tests for LUCENE-9037
 * This class needs a large heap to trigger Lucene indexing structure integer overflow.<p>
 * Use for example -Xmx20G -Xms20G in JVM args.<p>
 * From command line (in lucene-solr/lucene) you can run: ant test  -Dtestcase=TestIndexWriterTermsHashOverflow -Dtests.heapsize=20G -Dtests.asserts=true
 */
@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
@Monster("Slow, and needs 20G heap")
@TimeoutSuite(millis = 2 * TimeUnits.HOUR)
@Nightly
public class TestIndexWriterTermsHashOverflow extends LuceneTestCase {

  /**
   * A single large enough doc to index will cause overflow in internal Lucene data structures.
   * No easy work around this issue (but the doc really has to be huge!).
   * Indexing fails (and test passes since failure expected) with or without the fix of LUCENE-9037.
   */
  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testSingleLargeDocFails() throws Exception {
    // IOException is not going to be thrown, internal structure will overflow before and we'll see ArrayIndexOutOfBoundsException
    internalMultiDocsTest(130000000, 1,false);
  }

  /**
   * Multiple (different) large docs can cause overflow in internal data structures in case of IOException
   * Fails without the fix of LUCENE-9037, passes with it.
   */
  public void testManyLargeDifferentDocs() throws Exception {
    internalMultiDocsTest(20000000, 7,false);
  }

  /**
   * Multiple (identical) large docs can cause overflow in internal data structures in case of IOException.
   * Note we need more docs than when they're different because tokens are reused.
   * Fails without the fix of LUCENE-9037, passes with it.
   */
  public void testManyLargeIdenticalDocs() throws Exception {
    internalMultiDocsTest(20000000, 15,true);
  }

  /**
   * Multiple (different) small docs can cause overflow in internal data structures in case of IOException
   * Fails without the fix of LUCENE-9037, passes with it.
   */
  public void testManySmallDifferentDocs() throws Exception {
    internalMultiDocsTest(20000, 6352,false);
  }

  /**
   * Multiple (identical) small docs can cause overflow in internal data structures in case of IOException
   * Fails without the fix of LUCENE-9037, passes with it.
   */
  public void testManySmallIdenticalDocs() throws Exception {
    internalMultiDocsTest(20000, 29109,true);
  }

  /**
   * Helper method to test internal overflow with different doc sizes.
   * @param tokensPerDoc a way to vary the size of each doc submitted for indexing
   * @param nbDocs how many docs to add (while throwing IOException on each one during addition)
   * @param identicalDocs <code>false</code> if docs should all contain different tokens, <code>true</code> if docs should be identical.
   */
  private void internalMultiDocsTest(int tokensPerDoc, int nbDocs, boolean identicalDocs) throws Exception {
    // Analyzer will throw IOException on the last token of the doc
    IndexWriterConfig exceptionTokenStream = newIndexWriterConfig(new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        return new TokenStreamComponents(tokenizer, new TokenFilter(tokenizer) {
          private int count = 0;

          @Override
          public boolean incrementToken() throws IOException {
            if (++count >= tokensPerDoc - 1) {
              throw new IOException("Token count reached");
            }
            return input.incrementToken();
          }

          @Override
          public void reset() throws IOException {
            super.reset();
            this.count = 0;
          }
        });
      }
    });

    final FieldType NO_STORE = new FieldType(TextField.TYPE_NOT_STORED);

    Directory directory = newDirectory();
    IndexWriter indexWriter = new IndexWriter(directory, exceptionTokenStream);

    try {
      int startToken = 0;
      int limitToken = tokensPerDoc;
      for (int docnum = 1; docnum <= nbDocs; docnum++) {
        Document doc = new Document();
        doc.add(newField("content", manyTokens(startToken, limitToken), NO_STORE));

        try {
          indexWriter.addDocument(doc);
        } catch (IOException e) {
          // Do nothing: simulate caller getting IOException then coming back and adding documents again
        }

        // Increment token positions for generating next doc if docs should be different
        if (!identicalDocs) {
          startToken = limitToken;
          limitToken += tokensPerDoc;
        }
      }
    } finally {
      indexWriter.close();
      directory.close();
    }
  }

  private String manyTokens(int lowerbound, int upperbound) {
    StringBuilder sb = new StringBuilder();

    for (int i = lowerbound; i < upperbound; i++) {
      sb.append(getTokenForInt(i)).append(" ");
    }

    return sb.toString();
  }

  private String getTokenForInt(int v) {
    if (v <= 0) return "a";
    StringBuilder token = new StringBuilder();

    while (v > 0) {
      token.append((char) ('a' + v % 26));
      v /= 26;
    }

    return token.toString();
  }
}
