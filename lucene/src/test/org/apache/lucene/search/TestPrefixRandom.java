package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Create an index with random unicode terms
 * Generates random prefix queries, and validates against a simple impl.
 */
public class TestPrefixRandom extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.KEYWORD, false))
        .setMaxBufferedDocs(_TestUtil.nextInt(random, 50, 1000)));
    
    Document doc = new Document();
    Field bogus1 = newField("bogus", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
    Field field = newField("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    Field bogus2 = newField("zbogus", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
    doc.add(field);
    doc.add(bogus1);
    doc.add(bogus2);
    
    int num = atLeast(1000);

    for (int i = 0; i < num; i++) {
      field.setValue(_TestUtil.randomUnicodeString(random, 10));
      bogus1.setValue(_TestUtil.randomUnicodeString(random, 10));
      bogus2.setValue(_TestUtil.randomUnicodeString(random, 10));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    searcher.close();
    dir.close();
    super.tearDown();
  }
  
  /** a stupid prefix query that just blasts thru the terms */
  private class DumbPrefixQuery extends MultiTermQuery {
    private final Term prefix;
    
    DumbPrefixQuery(Term term) {
      super();
      prefix = term;
    }
    
    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SimplePrefixTermEnum(reader, prefix);
    }

    private class SimplePrefixTermEnum extends FilteredTermEnum {
      private final Term prefix;
      private boolean endEnum;

      private SimplePrefixTermEnum(IndexReader reader, Term prefix) throws IOException {
        this.prefix = prefix;
        setEnum(reader.terms(new Term(prefix.field(), "")));
      }

      @Override
      protected boolean termCompare(Term term) {
        if (term.field() == prefix.field()) {
          return term.text().startsWith(prefix.text());
        } else {
          endEnum = true;
          return false;
        }
      }

      @Override
      public float difference() {
        return 1.0F;
      }

      @Override
      protected boolean endEnum() {
        return endEnum;
      }
    }

    @Override
    public String toString(String field) {
      return field.toString() + ":" + prefix.toString();
    }
  }
  
  /** test a bunch of random prefixes */
  public void testPrefixes() throws Exception {
      int num = atLeast(100);
      for (int i = 0; i < num; i++)
        assertSame(_TestUtil.randomUnicodeString(random, 5));
  }
  
  /** check that the # of hits is the same as from a very
   * simple prefixquery implementation.
   */
  private void assertSame(String prefix) throws IOException {   
    PrefixQuery smart = new PrefixQuery(new Term("field", prefix));
    DumbPrefixQuery dumb = new DumbPrefixQuery(new Term("field", prefix));
    
    TopDocs smartDocs = searcher.search(smart, 25);
    TopDocs dumbDocs = searcher.search(dumb, 25);
    CheckHits.checkEqual(smart, smartDocs.scoreDocs, dumbDocs.scoreDocs);
  }
}
