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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.ParallelLeafReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests highlighting for matters *expressly* relating to term vectors.
 * <p>
 * This test DOES NOT represent all testing for highlighting when term vectors are used.  Other tests pick the offset
 * source at random (to include term vectors) and in-effect test term vectors generally.
 */
public class TestUnifiedHighlighterTermVec extends LuceneTestCase {

  private Analyzer indexAnalyzer;
  private Directory dir;

  @Before
  public void doBefore() throws IOException {
    indexAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase
    dir = newDirectory();
  }

  @After
  public void doAfter() throws IOException {
    dir.close();
  }

  public void testFetchTermVecsOncePerDoc() throws IOException {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    // Declare some number of fields with random field type; but at least one will have term vectors.
    final int numTvFields = 1 + random().nextInt(3);
    List<String> fields = new ArrayList<>(numTvFields);
    List<FieldType> fieldTypes = new ArrayList<>(numTvFields);
    for (int i = 0; i < numTvFields; i++) {
      fields.add("body" + i);
      fieldTypes.add(UHTestHelper.randomFieldType(random()));
    }
    //ensure at least one has TVs by setting one randomly to it:
    fieldTypes.set(random().nextInt(fieldTypes.size()), UHTestHelper.tvType);

    final int numDocs = 1 + random().nextInt(3);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (String field : fields) {
        doc.add(new Field(field, "some test text", UHTestHelper.tvType));
      }
      iw.addDocument(doc);
    }

    // Wrap the reader to ensure we only fetch TVs once per doc
    DirectoryReader originalReader = iw.getReader();
    IndexReader ir = new AssertOnceTermVecDirectoryReader(originalReader);
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
    for (String field : fields) {
      queryBuilder.add(new TermQuery(new Term(field, "test")), BooleanClause.Occur.MUST);
    }
    BooleanQuery query = queryBuilder.build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(numDocs, topDocs.totalHits.value);
    Map<String, String[]> fieldToSnippets =
        highlighter.highlightFields(fields.toArray(new String[numTvFields]), query, topDocs);
    String[] expectedSnippetsByDoc = new String[numDocs];
    Arrays.fill(expectedSnippetsByDoc, "some <b>test</b> text");
    for (String field : fields) {
      assertArrayEquals(expectedSnippetsByDoc, fieldToSnippets.get(field));
    }

    ir.document(0); // ensure this works because the ir hasn't been closed
    ir.close();
  }

  private static class AssertOnceTermVecDirectoryReader extends FilterDirectoryReader {
    static final SubReaderWrapper SUB_READER_WRAPPER = new SubReaderWrapper() {
      @Override
      public LeafReader wrap(LeafReader reader) {
        return new FilterLeafReader(reader) {
          BitSet seenDocIDs = new BitSet();

          @Override
          public Fields getTermVectors(int docID) throws IOException {
            // if we're invoked by ParallelLeafReader then we can't do our assertion. TODO see LUCENE-6868
            if (callStackContains(ParallelLeafReader.class) == false
                && callStackContains(CheckIndex.class) == false) {
              assertFalse("Should not request TVs for doc more than once.", seenDocIDs.get(docID));
              seenDocIDs.set(docID);
            }

            return super.getTermVectors(docID);
          }

          @Override
          public CacheHelper getCoreCacheHelper() {
            return null;
          }

          @Override
          public CacheHelper getReaderCacheHelper() {
            return null;
          }
        };
      }
    };

    AssertOnceTermVecDirectoryReader(DirectoryReader in) throws IOException {
      super(in, SUB_READER_WRAPPER);
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new AssertOnceTermVecDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUserFailedToIndexOffsets() throws IOException {
    FieldType fieldType = new FieldType(UHTestHelper.tvType); // note: it's indexed too
    fieldType.setStoreTermVectorPositions(random().nextBoolean());
    fieldType.setStoreTermVectorOffsets(false);

    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);
    Document doc = new Document();
    doc.add(new Field("body", "term vectors", fieldType));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected Set<HighlightFlag> getFlags(String field) {
        return Collections.emptySet();//no WEIGHT_MATCHES
      }
    };
    TermQuery query = new TermQuery(new Term("body", "vectors"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    try {
      highlighter.highlight("body", query, topDocs, 1);//should throw
    } finally {
      ir.close();
    }
  }

}
