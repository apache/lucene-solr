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
package org.apache.lucene.search.suggest.document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilter;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.suggest.BitsProducer;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.core.IsEqual.equalTo;

public class TestSuggestField extends LuceneTestCase {

  public Directory dir;

  @Before
  public void before() throws Exception {
    dir = newDirectory();
  }

  @After
  public void after() throws Exception {
    dir.close();
  }

  @Test
  public void testEmptySuggestion() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new SuggestField("suggest_field", "", 3);
    });
    assertTrue(expected.getMessage().contains("value"));
  }

  @Test
  public void testNegativeWeight() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new SuggestField("suggest_field", "sugg", -1);
    });
    assertTrue(expected.getMessage().contains("weight"));
  }

  @Test
  public void testReservedChars() throws Exception {
    CharsRefBuilder charsRefBuilder = new CharsRefBuilder();
    charsRefBuilder.append("sugg");
    charsRefBuilder.setCharAt(2, (char) ConcatenateGraphFilter.SEP_LABEL);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new SuggestField("name", charsRefBuilder.toString(), 1);
    });
    assertTrue(expected.getMessage().contains("[0x1f]"));

    charsRefBuilder.setCharAt(2, (char) CompletionAnalyzer.HOLE_CHARACTER);
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new SuggestField("name", charsRefBuilder.toString(), 1);
    });
    assertTrue(expected.getMessage().contains("[0x1e]"));

    charsRefBuilder.setCharAt(2, (char) NRTSuggesterBuilder.END_BYTE);
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new SuggestField("name", charsRefBuilder.toString(), 1);
    });
    assertTrue(expected.getMessage().contains("[0x0]"));
  }

  @Test
  public void testEmpty() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab"));
    TopSuggestDocs lookupDocs = suggestIndexSearcher.suggest(query, 3, false);
    assertThat(lookupDocs.totalHits.value, equalTo(0L));
    reader.close();
    iw.close();
  }

  @Test
  public void testTokenStream() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    SuggestField suggestField = new SuggestField("field", "input", 1);
    BytesRef surfaceForm = new BytesRef("input");
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream)) {
      output.writeVInt(surfaceForm.length);
      output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
      output.writeVInt(1 + 1);
      output.writeByte(SuggestField.TYPE);
    }
    BytesRef payload = new BytesRef(byteArrayOutputStream.toByteArray());
    TokenStream stream = new PayloadAttrToTypeAttrFilter(suggestField.tokenStream(analyzer, null));
    assertTokenStreamContents(stream, new String[] {"input"}, null, null, new String[]{payload.utf8ToString()}, new int[]{1}, null, null);

    CompletionAnalyzer completionAnalyzer = new CompletionAnalyzer(analyzer);
    stream = new PayloadAttrToTypeAttrFilter(suggestField.tokenStream(completionAnalyzer, null));
    assertTokenStreamContents(stream, new String[] {"input"}, null, null, new String[]{payload.utf8ToString()}, new int[]{1}, null, null);
  }

  @Test @Slow
  public void testDupSuggestFieldValues() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    final int num = Math.min(1000, atLeast(100));
    int[] weights = new int[num];
    for(int i = 0; i < num; i++) {
      Document document = new Document();
      weights[i] = random().nextInt(Integer.MAX_VALUE);
      document.add(new SuggestField("suggest_field", "abc", weights[i]));
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    Entry[] expectedEntries = new Entry[num];
    Arrays.sort(weights);
    for (int i = 1; i <= num; i++) {
      expectedEntries[i - 1] = new Entry("abc", weights[num - i]);
    }

    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc"));
    TopSuggestDocs lookupDocs = suggestIndexSearcher.suggest(query, num, false);
    assertSuggestions(lookupDocs, expectedEntries);

    reader.close();
    iw.close();
  }

  public void testDeduplication() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    final int num = TestUtil.nextInt(random(), 2, 20);
    int[] weights = new int[num];
    int bestABCWeight = Integer.MIN_VALUE;
    int bestABDWeight = Integer.MIN_VALUE;
    for(int i = 0; i < num; i++) {
      Document document = new Document();
      weights[i] = random().nextInt(Integer.MAX_VALUE);
      String suggestValue;
      boolean doABC;
      if (i == 0) {
        doABC = true;
      } else if (i == 1) {
        doABC = false;
      } else {
        doABC = random().nextBoolean();
      }
      if (doABC) {
        suggestValue = "abc";
        bestABCWeight = Math.max(bestABCWeight, weights[i]);
      } else {
        suggestValue = "abd";
        bestABDWeight = Math.max(bestABDWeight, weights[i]);
      }
      document.add(new SuggestField("suggest_field", suggestValue, weights[i]));
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    Entry[] expectedEntries = new Entry[2];
    if (bestABDWeight > bestABCWeight) {
      expectedEntries[0] = new Entry("abd", bestABDWeight);
      expectedEntries[1] = new Entry("abc", bestABCWeight);
    } else {
      expectedEntries[0] = new Entry("abc", bestABCWeight);
      expectedEntries[1] = new Entry("abd", bestABDWeight);
    }

    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "a"));
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(2, true);
    suggestIndexSearcher.suggest(query, collector);
    TopSuggestDocs lookupDocs = collector.get();
    assertSuggestions(lookupDocs, expectedEntries);

    reader.close();
    iw.close();
  }

  @Slow
  public void testExtremeDeduplication() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    final int num = atLeast(500);
    int bestWeight = Integer.MIN_VALUE;
    for(int i = 0; i < num; i++) {
      Document document = new Document();
      int weight = TestUtil.nextInt(random(), 10, 100);
      bestWeight = Math.max(weight, bestWeight);
      document.add(new SuggestField("suggest_field", "abc", weight));
      iw.addDocument(document);
      if (rarely()) {
        iw.commit();
      }
    }
    Document document = new Document();
    document.add(new SuggestField("suggest_field", "abd", 7));
    iw.addDocument(document);

    if (random().nextBoolean()) {
      iw.forceMerge(1);
    }

    DirectoryReader reader = iw.getReader();
    Entry[] expectedEntries = new Entry[2];
    expectedEntries[0] = new Entry("abc", bestWeight);
    expectedEntries[1] = new Entry("abd", 7);

    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "a"));
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(2, true);
    suggestIndexSearcher.suggest(query, collector);
    TopSuggestDocs lookupDocs = collector.get();
    assertSuggestions(lookupDocs, expectedEntries);

    reader.close();
    iw.close();
  }
  
  private static String randomSimpleString(int numDigits, int maxLen) {
    final int len = TestUtil.nextInt(random(), 1, maxLen);
    final char[] chars = new char[len];
    for(int j=0;j<len;j++) {
      chars[j] = (char) ('a' + random().nextInt(numDigits));
    }
    return new String(chars);
  }

  public void testRandom() throws Exception {
    int numDigits = TestUtil.nextInt(random(), 1, 6);
    Set<String> keys = new HashSet<>();
    int keyCount = TestUtil.nextInt(random(), 1, 20);
    if (numDigits == 1) {
      keyCount = Math.min(9, keyCount);
    }
    while (keys.size() < keyCount) {
      keys.add(randomSimpleString(numDigits, 10));
    }
    List<String> keysList = new ArrayList<>(keys);

    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = iwcWithSuggestField(analyzer, "suggest_field");
    // we rely on docID order:
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    int docCount = TestUtil.nextInt(random(), 1, 200);
    Entry[] docs = new Entry[docCount];
    for(int i=0;i<docCount;i++) {
      int weight = random().nextInt(40);
      String key = keysList.get(random().nextInt(keyCount));
      //System.out.println("KEY: " + key);
      docs[i] = new Entry(key, null, weight, i);
      Document doc = new Document();
      doc.add(new SuggestField("suggest_field", key, weight));
      iw.addDocument(doc);
      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher searcher = new SuggestIndexSearcher(reader);

    int iters = atLeast(200);
    for(int iter=0;iter<iters;iter++) {
      String prefix = randomSimpleString(numDigits, 2);
      if (VERBOSE) {
        System.out.println("\nTEST: prefix=" + prefix);
      }

      // slow but hopefully correct suggester:
      List<Entry> expected = new ArrayList<>();
      for(Entry doc : docs) {
        if (doc.output.startsWith(prefix)) {
          expected.add(doc);
        }
      }
      Collections.sort(expected,
          (a, b) -> {
            // sort by higher score:
            int cmp = Float.compare(b.value, a.value);
            if (cmp == 0) {
              // tie break by completion key
              cmp = Lookup.CHARSEQUENCE_COMPARATOR.compare(a.output, b.output);
              if (cmp == 0) {
                // prefer smaller doc id, in case of a tie
                cmp = Integer.compare(a.id, b.id);
              }
            }
            return cmp;
          });

      boolean dedup = random().nextBoolean();
      if (dedup) {
        List<Entry> deduped = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for(Entry entry : expected) {
          if (seen.contains(entry.output) == false) {
            seen.add(entry.output);
            deduped.add(entry);
          }
        }
        expected = deduped;
      }

      // TODO: re-enable this, except something is buggy about tie breaks at the topN threshold now:
      //int topN = TestUtil.nextInt(random(), 1, docCount+10);
      int topN = docCount;
      
      if (VERBOSE) {
        if (dedup) {
          System.out.println("  expected (dedup'd) topN=" + topN + ":");
        } else {
          System.out.println("  expected topN=" + topN + ":");
        }
        for(int i=0;i<expected.size();i++) {
          if (i >= topN) {
            System.out.println("    leftover: " + i + ": " + expected.get(i));
          } else {
            System.out.println("    " + i + ": " + expected.get(i));
          }
        }
      }
      expected = expected.subList(0, Math.min(topN, expected.size()));
      
      PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", prefix));
      TopSuggestDocsCollector collector = new TopSuggestDocsCollector(topN, dedup);
      searcher.suggest(query, collector);
      TopSuggestDocs actual = collector.get();
      if (VERBOSE) {
        System.out.println("  actual:");
        SuggestScoreDoc[] suggestScoreDocs = (SuggestScoreDoc[]) actual.scoreDocs;
        for(int i=0;i<suggestScoreDocs.length;i++) {
          System.out.println("    " + i + ": " + suggestScoreDocs[i]);
        }
      }

      assertSuggestions(actual, expected.toArray(new Entry[expected.size()]));
    }
    
    reader.close();
    iw.close();
  }

  @Test
  public void testNRTDeletedDocFiltering() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    // using IndexWriter instead of RandomIndexWriter
    IndexWriter iw = new IndexWriter(dir, iwcWithSuggestField(analyzer, "suggest_field"));

    int num = Math.min(1000, atLeast(10));

    int numLive = 0;
    List<Entry> expectedEntries = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, num - i));
      if (i % 2 == 0) {
        document.add(newStringField("str_field", "delete", Field.Store.YES));
      } else {
        numLive++;
        expectedEntries.add(new Entry("abc_" + i, num - i));
        document.add(newStringField("str_field", "no_delete", Field.Store.YES));
      }
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    iw.deleteDocuments(new Term("str_field", "delete"));

    DirectoryReader reader = DirectoryReader.open(iw);
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, numLive, false);
    assertSuggestions(suggest, expectedEntries.toArray(new Entry[expectedEntries.size()]));

    reader.close();
    iw.close();
  }

  @Test
  public void testSuggestOnAllFilteredDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, i));
      document.add(newStringField("str_fld", "deleted", Field.Store.NO));
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    BitsProducer filter = new BitsProducer() {
      @Override
      public Bits getBits(LeafReaderContext context) throws IOException {
        return new Bits.MatchNoBits(context.reader().maxDoc());
      }
    };
    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    // no random access required;
    // calling suggest with filter that does not match any documents should early terminate
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"), filter);
    TopSuggestDocs suggest = indexSearcher.suggest(query, num, false);
    assertThat(suggest.totalHits.value, equalTo(0L));
    reader.close();
    iw.close();
  }

  @Test
  public void testSuggestOnAllDeletedDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    // using IndexWriter instead of RandomIndexWriter
    IndexWriter iw = new IndexWriter(dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, i));
      document.add(newStringField("delete", "delete", Field.Store.NO));
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    iw.deleteDocuments(new Term("delete", "delete"));

    DirectoryReader reader = DirectoryReader.open(iw);
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, num, false);
    assertThat(suggest.totalHits.value, equalTo(0L));

    reader.close();
    iw.close();
  }

  @Test
  public void testSuggestOnMostlyDeletedDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    // using IndexWriter instead of RandomIndexWriter
    IndexWriter iw = new IndexWriter(dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 1; i <= num; i++) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, i));
      document.add(new StoredField("weight_fld", i));
      document.add(new IntPoint("weight_fld", i));
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    iw.deleteDocuments(IntPoint.newRangeQuery("weight_fld", 2, Integer.MAX_VALUE));

    DirectoryReader reader = DirectoryReader.open(iw);
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, 1, false);
    assertSuggestions(suggest, new Entry("abc_1", 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultipleSuggestFieldsPerDoc() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "sug_field_1", "sug_field_2"));

    Document document = new Document();
    document.add(new SuggestField("sug_field_1", "apple", 4));
    document.add(new SuggestField("sug_field_2", "april", 3));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("sug_field_1", "aples", 3));
    document.add(new SuggestField("sug_field_2", "apartment", 2));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();

    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("sug_field_1", "ap"));
    TopSuggestDocs suggestDocs1 = suggestIndexSearcher.suggest(query, 4, false);
    assertSuggestions(suggestDocs1, new Entry("apple", 4), new Entry("aples", 3));
    query = new PrefixCompletionQuery(analyzer, new Term("sug_field_2", "ap"));
    TopSuggestDocs suggestDocs2 = suggestIndexSearcher.suggest(query, 4, false);
    assertSuggestions(suggestDocs2, new Entry("april", 3), new Entry("apartment", 2));

    // check that the doc ids are consistent
    for (int i = 0; i < suggestDocs1.scoreDocs.length; i++) {
      ScoreDoc suggestScoreDoc = suggestDocs1.scoreDocs[i];
      assertThat(suggestScoreDoc.doc, equalTo(suggestDocs2.scoreDocs[i].doc));
    }

    reader.close();
    iw.close();
  }

  @Test
  public void testEarlyTermination() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));

    // have segments of 4 documents
    // with descending suggestion weights
    // suggest should early terminate for
    // segments with docs having lower suggestion weights
    for (int i = num; i > 0; i--) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, i));
      iw.addDocument(document);
      if (i % 4 == 0) {
        iw.commit();
      }
    }
    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, 1, false);
    assertSuggestions(suggest, new Entry("abc_" + num, num));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultipleSegments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    List<Entry> entries = new ArrayList<>();

    // ensure at least some segments have no suggest field
    for (int i = num; i > 0; i--) {
      Document document = new Document();
      if (random().nextInt(4) == 1) {
        document.add(new SuggestField("suggest_field", "abc_" + i, i));
        entries.add(new Entry("abc_" + i, i));
      }
      document.add(new StoredField("weight_fld", i));
      iw.addDocument(document);
      if (usually()) {
        iw.commit();
      }
    }
    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, (entries.size() == 0) ? 1 : entries.size(), false);
    assertSuggestions(suggest, entries.toArray(new Entry[entries.size()]));

    reader.close();
    iw.close();
  }


  @Test
  public void testReturnedDocID() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, num));
      document.add(new StoredField("int_field", i));
      iw.addDocument(document);

      if (random().nextBoolean()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, num, false);
    assertEquals(num, suggest.totalHits.value);
    for (SuggestScoreDoc suggestScoreDoc : suggest.scoreLookupDocs()) {
      String key = suggestScoreDoc.key.toString();
      assertTrue(key.startsWith("abc_"));
      String substring = key.substring(4);
      int fieldValue = Integer.parseInt(substring);
      Document doc = reader.document(suggestScoreDoc.doc);
      assertEquals(doc.getField("int_field").numericValue().intValue(), fieldValue);
    }

    reader.close();
    iw.close();
  }

  @Test
  public void testScoring() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    int num = Math.min(1000, atLeast(50));
    String[] prefixes = {"abc", "bac", "cab"};
    Map<String, Integer> mappings = new HashMap<>();
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      String suggest = prefixes[i % 3] + TestUtil.randomSimpleString(random(), 10) + "_" +String.valueOf(i);
      int weight = random().nextInt(Integer.MAX_VALUE);
      document.add(new SuggestField("suggest_field", suggest, weight));
      mappings.put(suggest, weight);
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    for (String prefix : prefixes) {
      PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", prefix));
      TopSuggestDocs suggest = indexSearcher.suggest(query, num, false);
      assertTrue(suggest.totalHits.value > 0);
      float topScore = -1;
      for (SuggestScoreDoc scoreDoc : suggest.scoreLookupDocs()) {
        if (topScore != -1) {
          assertTrue(topScore >= scoreDoc.score);
        }
        topScore = scoreDoc.score;
        assertThat((float) mappings.get(scoreDoc.key.toString()), equalTo(scoreDoc.score));
        assertNotNull(mappings.remove(scoreDoc.key.toString()));
      }
    }

    assertThat(mappings.size(), equalTo(0));
    reader.close();
    iw.close();
  }

  @Test
  public void testRealisticKeys() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    LineFileDocs lineFileDocs = new LineFileDocs(random());
    int num = Math.min(1000, atLeast(50));
    Map<String, Integer> mappings = new HashMap<>();
    for (int i = 0; i < num; i++) {
      Document document = lineFileDocs.nextDoc();
      String title = document.getField("title").stringValue();
      int maxLen = Math.min(title.length(), 500);
      String prefix = title.substring(0, maxLen);
      int weight = random().nextInt(Integer.MAX_VALUE);
      Integer prevWeight = mappings.get(prefix);
      if (prevWeight == null || prevWeight < weight) {
        mappings.put(prefix, weight);
      }
      Document doc = new Document();
      doc.add(new SuggestField("suggest_field", prefix, weight));
      iw.addDocument(doc);

      if (rarely()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);

    for (Map.Entry<String, Integer> entry : mappings.entrySet()) {
      String title = entry.getKey();

      PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", title));
      TopSuggestDocs suggest = indexSearcher.suggest(query, mappings.size(), false);
      assertTrue(suggest.totalHits.value > 0);
      boolean matched = false;
      for (ScoreDoc scoreDoc : suggest.scoreDocs) {
        matched = Float.compare(scoreDoc.score, (float) entry.getValue()) == 0;
        if (matched) {
          break;
        }
      }
      assertTrue("at least one of the entries should have the score", matched);
    }
    lineFileDocs.close();
    reader.close();
    iw.close();
  }

  @Test
  public void testThreads() throws Exception {
    final Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field_1", "suggest_field_2", "suggest_field_3"));
    int num = Math.min(1000, atLeast(100));
    final String prefix1 = "abc1_";
    final String prefix2 = "abc2_";
    final String prefix3 = "abc3_";
    final Entry[] entries1 = new Entry[num];
    final Entry[] entries2 = new Entry[num];
    final Entry[] entries3 = new Entry[num];
    for (int i = 0; i < num; i++) {
      int weight = num - (i + 1);
      entries1[i] = new Entry(prefix1 + weight, weight);
      entries2[i] = new Entry(prefix2 + weight, weight);
      entries3[i] = new Entry(prefix3 + weight, weight);
    }
    for (int i = 0; i < num; i++) {
      Document doc = new Document();
      doc.add(new SuggestField("suggest_field_1", prefix1 + i, i));
      doc.add(new SuggestField("suggest_field_2", prefix2 + i, i));
      doc.add(new SuggestField("suggest_field_3", prefix3 + i, i));
      iw.addDocument(doc);

      if (rarely()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    int numThreads = TestUtil.nextInt(random(), 2, 7);
    Thread threads[] = new Thread[numThreads];
    final CyclicBarrier startingGun = new CyclicBarrier(numThreads+1);
    final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
    final SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            startingGun.await();
            PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_1", prefix1));
            TopSuggestDocs suggest = indexSearcher.suggest(query, num, false);
            assertSuggestions(suggest, entries1);
            query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_2", prefix2));
            suggest = indexSearcher.suggest(query, num, false);
            assertSuggestions(suggest, entries2);
            query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_3", prefix3));
            suggest = indexSearcher.suggest(query, num, false);
            assertSuggestions(suggest, entries3);
          } catch (Throwable e) {
            errors.add(e);
          }
        }
      };
      threads[i].start();
    }

    startingGun.await();
    for (Thread t : threads) {
      t.join();
    }
    assertTrue(errors.toString(), errors.isEmpty());

    reader.close();
    iw.close();
  }

  static class Entry {
    final String output;
    final float value;
    final String context;
    final int id;

    Entry(String output, float value) {
      this(output, null, value);
    }

    Entry(String output, String context, float value) {
      this(output, context, value, -1);
    }

    Entry(String output, String context, float value, int id) {
      this.output = output;
      this.value = value;
      this.context = context;
      this.id = id;
    }

    @Override
    public String toString() {
      return "key=" + output + " score=" + value + " context=" + context + " id=" + id;
    }
  }

  static void assertSuggestions(TopDocs actual, Entry... expected) {
    SuggestScoreDoc[] suggestScoreDocs = (SuggestScoreDoc[]) actual.scoreDocs;
    for (int i = 0; i < Math.min(expected.length, suggestScoreDocs.length); i++) {
      SuggestScoreDoc lookupDoc = suggestScoreDocs[i];
      String msg = "Hit " + i + ": expected: " + toString(expected[i]) + " but actual: " + toString(lookupDoc);
      assertThat(msg, lookupDoc.key.toString(), equalTo(expected[i].output));
      assertThat(msg, lookupDoc.score, equalTo(expected[i].value));
      assertThat(msg, lookupDoc.context, equalTo(expected[i].context));
    }
    assertThat(suggestScoreDocs.length, equalTo(expected.length));
  }

  private static String toString(Entry expected) {
    return "key:"+ expected.output+" score:"+expected.value+" context:"+expected.context;
  }

  private static String toString(SuggestScoreDoc actual) {
    return "key:"+ actual.key.toString()+" score:"+actual.score+" context:"+actual.context;
  }

  static IndexWriterConfig iwcWithSuggestField(Analyzer analyzer, String... suggestFields) {
    return iwcWithSuggestField(analyzer, asSet(suggestFields));
  }

  static IndexWriterConfig iwcWithSuggestField(Analyzer analyzer, final Set<String> suggestFields) {
    IndexWriterConfig iwc = newIndexWriterConfig(random(), analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    Codec filterCodec = new Lucene87Codec() {
      CompletionPostingsFormat.FSTLoadMode fstLoadMode =
          RandomPicks.randomFrom(random(), CompletionPostingsFormat.FSTLoadMode.values());
      PostingsFormat postingsFormat = new Completion84PostingsFormat(fstLoadMode);

      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        if (suggestFields.contains(field)) {
          return postingsFormat;
        }
        return super.getPostingsFormatForField(field);
      }
    };
    iwc.setCodec(filterCodec);
    return iwc;
  }

  public final static class PayloadAttrToTypeAttrFilter extends TokenFilter {
    private PayloadAttribute payload = addAttribute(PayloadAttribute.class);
    private TypeAttribute type = addAttribute(TypeAttribute.class);

    protected PayloadAttrToTypeAttrFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        // we move them over so we can assert them more easily in the tests
        type.setType(payload.getPayload().utf8ToString());
        return true;
      }
      return false;
    }
  }
}
