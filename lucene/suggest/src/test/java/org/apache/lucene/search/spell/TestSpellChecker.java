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
package org.apache.lucene.search.spell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Spell checker test case
 */
public class TestSpellChecker extends LuceneTestCase {
  private SpellCheckerMock spellChecker;
  private Directory userindex, spellindex;
  private Analyzer analyzer;
  private List<IndexSearcher> searchers;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    //create a user index
    userindex = newDirectory();
    analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(userindex, new IndexWriterConfig(analyzer));

    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(newTextField("field1", English.intToEnglish(i), Field.Store.YES));
      doc.add(newTextField("field2", English.intToEnglish(i + 1), Field.Store.YES)); // + word thousand
      doc.add(newTextField("field3", "fvei" + (i % 2 == 0 ? " five" : ""), Field.Store.YES)); // + word thousand
      writer.addDocument(doc);
    }
    {
      Document doc = new Document();
      doc.add(newTextField("field1", "eight", Field.Store.YES)); // "eight" in
                                                                   // the index
                                                                   // twice
      writer.addDocument(doc);
    }
    {
      Document doc = new Document();
      doc
          .add(newTextField("field1", "twenty-one twenty-one", Field.Store.YES)); // "twenty-one" in the index thrice
      writer.addDocument(doc);
    }
    {
      Document doc = new Document();
      doc.add(newTextField("field1", "twenty", Field.Store.YES)); // "twenty"
                                                                    // in the
                                                                    // index
                                                                    // twice
      writer.addDocument(doc);
    }
    
    writer.close();
    searchers = Collections.synchronizedList(new ArrayList<IndexSearcher>());
    // create the spellChecker
    spellindex = newDirectory();
    spellChecker = new SpellCheckerMock(spellindex);
  }
  
  @Override
  public void tearDown() throws Exception {
    userindex.close();
    if (!spellChecker.isClosed())
      spellChecker.close();
    spellindex.close();
    analyzer.close();
    super.tearDown();
  }


  public void testBuild() throws IOException {
    IndexReader r = DirectoryReader.open(userindex);

    spellChecker.clearIndex();

    addwords(r, spellChecker, "field1");
    int num_field1 = this.numdoc();

    addwords(r, spellChecker, "field2");
    int num_field2 = this.numdoc();

    assertEquals(num_field2, num_field1 + 1);
    
    assertLastSearcherOpen(4);
    
    checkCommonSuggestions(r);
    checkLevenshteinSuggestions(r);
    
    spellChecker.setStringDistance(new JaroWinklerDistance());
    spellChecker.setAccuracy(0.8f);
    checkCommonSuggestions(r);
    checkJaroWinklerSuggestions();
    // the accuracy is set to 0.8 by default, but the best result has a score of 0.925
    String[] similar = spellChecker.suggestSimilar("fvie", 2, 0.93f);
    assertTrue(similar.length == 0);
    similar = spellChecker.suggestSimilar("fvie", 2, 0.92f);
    assertTrue(similar.length == 1);

    similar = spellChecker.suggestSimilar("fiv", 2);
    assertTrue(similar.length > 0);
    assertEquals(similar[0], "five");
    
    spellChecker.setStringDistance(new NGramDistance(2));
    spellChecker.setAccuracy(0.5f);
    checkCommonSuggestions(r);
    checkNGramSuggestions();

    r.close();
  }

  public void testComparator() throws Exception {
    IndexReader r = DirectoryReader.open(userindex);
    Directory compIdx = newDirectory();
    SpellChecker compareSP = new SpellCheckerMock(compIdx, new LevenshteinDistance(), new SuggestWordFrequencyComparator());
    addwords(r, compareSP, "field3");

    String[] similar = compareSP.suggestSimilar("fvie", 2, r, "field3",
        SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
    assertTrue(similar.length == 2);
    //five and fvei have the same score, but different frequencies.
    assertEquals("fvei", similar[0]);
    assertEquals("five", similar[1]);
    r.close();
    if (!compareSP.isClosed())
      compareSP.close();
    compIdx.close();
  }
  
  public void testBogusField() throws Exception {
    IndexReader r = DirectoryReader.open(userindex);
    Directory compIdx = newDirectory();
    SpellChecker compareSP = new SpellCheckerMock(compIdx, new LevenshteinDistance(), new SuggestWordFrequencyComparator());
    addwords(r, compareSP, "field3");

    String[] similar = compareSP.suggestSimilar("fvie", 2, r,
        "bogusFieldBogusField", SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
    assertEquals(0, similar.length);
    r.close();
    if (!compareSP.isClosed())
      compareSP.close();
    compIdx.close();
  }
  
  public void testSuggestModes() throws Exception {
    IndexReader r = DirectoryReader.open(userindex);
    spellChecker.clearIndex();
    addwords(r, spellChecker, "field1");
    
    {
      String[] similar = spellChecker.suggestSimilar("eighty", 2, r, "field1",
          SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
      assertEquals(1, similar.length);
      assertEquals("eighty", similar[0]);
    }
    
    {
      String[] similar = spellChecker.suggestSimilar("eight", 2, r, "field1",
          SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
      assertEquals(1, similar.length);
      assertEquals("eight", similar[0]);
    }
    
    {
      String[] similar = spellChecker.suggestSimilar("eighty", 5, r, "field1",
          SuggestMode.SUGGEST_MORE_POPULAR);
      assertEquals(5, similar.length);
      assertEquals("eight", similar[0]);
    }
    
    {
      String[] similar = spellChecker.suggestSimilar("twenty", 5, r, "field1",
          SuggestMode.SUGGEST_MORE_POPULAR);
      assertEquals(1, similar.length);
      assertEquals("twenty-one", similar[0]);
    }
    
    {
      String[] similar = spellChecker.suggestSimilar("eight", 5, r, "field1",
          SuggestMode.SUGGEST_MORE_POPULAR);
      assertEquals(0, similar.length);
    }
    
    {
      String[] similar = spellChecker.suggestSimilar("eighty", 5, r, "field1",
          SuggestMode.SUGGEST_ALWAYS);
      assertEquals(5, similar.length);
      assertEquals("eight", similar[0]);
    }
    
    {
      String[] similar = spellChecker.suggestSimilar("eight", 5, r, "field1",
          SuggestMode.SUGGEST_ALWAYS);
      assertEquals(5, similar.length);
      assertEquals("eighty", similar[0]);
    }
    r.close();
  }
  private void checkCommonSuggestions(IndexReader r) throws IOException {
    String[] similar = spellChecker.suggestSimilar("fvie", 2);
    assertTrue(similar.length > 0);
    assertEquals(similar[0], "five");
    
    similar = spellChecker.suggestSimilar("five", 2);
    if (similar.length > 0) {
      assertFalse(similar[0].equals("five")); // don't suggest a word for itself
    }
    
    similar = spellChecker.suggestSimilar("fiv", 2);
    assertTrue(similar.length > 0);
    assertEquals(similar[0], "five");
    
    similar = spellChecker.suggestSimilar("fives", 2);
    assertTrue(similar.length > 0);
    assertEquals(similar[0], "five");
    
    assertTrue(similar.length > 0);
    similar = spellChecker.suggestSimilar("fie", 2);
    assertEquals(similar[0], "five");
    
    //  test restraint to a field
    similar = spellChecker.suggestSimilar("tousand", 10, r, "field1",
        SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
    assertEquals(0, similar.length); // there isn't the term thousand in the field field1

    similar = spellChecker.suggestSimilar("tousand", 10, r, "field2",
        SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
    assertEquals(1, similar.length); // there is the term thousand in the field field2
  }

  private void checkLevenshteinSuggestions(IndexReader r) throws IOException {
    // test small word
    String[] similar = spellChecker.suggestSimilar("fvie", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    similar = spellChecker.suggestSimilar("five", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "nine");     // don't suggest a word for itself

    similar = spellChecker.suggestSimilar("fiv", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    similar = spellChecker.suggestSimilar("ive", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "five");
    assertEquals(similar[1], "nine");

    similar = spellChecker.suggestSimilar("fives", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    similar = spellChecker.suggestSimilar("fie", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "five");
    assertEquals(similar[1], "nine");
    
    similar = spellChecker.suggestSimilar("fi", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    // test restraint to a field
    similar = spellChecker.suggestSimilar("tousand", 10, r, "field1",
        SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
    assertEquals(0, similar.length); // there isn't the term thousand in the field field1

    similar = spellChecker.suggestSimilar("tousand", 10, r, "field2",
        SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
    assertEquals(1, similar.length); // there is the term thousand in the field field2
    
    similar = spellChecker.suggestSimilar("onety", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "ninety");
    assertEquals(similar[1], "one");
    // should not throw exception
    spellChecker.suggestSimilar("tousand", 10, r, null, SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
  }

  private void checkJaroWinklerSuggestions() throws IOException {
    String[] similar = spellChecker.suggestSimilar("onety", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "one");
    assertEquals(similar[1], "ninety");
  }
  
  private void checkNGramSuggestions() throws IOException {
    String[] similar = spellChecker.suggestSimilar("onety", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "one");
    assertEquals(similar[1], "ninety");
  }

  private void addwords(IndexReader r, SpellChecker sc, String field) throws IOException {
    long time = System.currentTimeMillis();
    sc.indexDictionary(new LuceneDictionary(r, field), newIndexWriterConfig(null), false);
    time = System.currentTimeMillis() - time;
    //System.out.println("time to build " + field + ": " + time);
  }

  private int numdoc() throws IOException {
    IndexReader rs = DirectoryReader.open(spellindex);
    int num = rs.numDocs();
    assertTrue(num != 0);
    //System.out.println("num docs: " + num);
    rs.close();
    return num;
  }
  
  public void testClose() throws IOException {
    IndexReader r = DirectoryReader.open(userindex);
    spellChecker.clearIndex();
    String field = "field1";
    addwords(r, spellChecker, "field1");
    int num_field1 = this.numdoc();
    addwords(r, spellChecker, "field2");
    int num_field2 = this.numdoc();
    assertEquals(num_field2, num_field1 + 1);
    checkCommonSuggestions(r);
    assertLastSearcherOpen(4);
    spellChecker.close();
    assertSearchersClosed();

    expectThrows(AlreadyClosedException.class, () -> {
      spellChecker.close();
    });

    expectThrows(AlreadyClosedException.class, () -> {
      checkCommonSuggestions(r);
    });
    
    expectThrows(AlreadyClosedException.class, () -> {
      spellChecker.clearIndex();
    });
    
    expectThrows(AlreadyClosedException.class, () -> {
      spellChecker.indexDictionary(new LuceneDictionary(r, field), newIndexWriterConfig(null), false);
    });
    
    expectThrows(AlreadyClosedException.class, () -> {
      spellChecker.setSpellIndex(spellindex);
    });

    assertEquals(4, searchers.size());
    assertSearchersClosed();
    r.close();
  }
  
  /*
   * tests if the internally shared indexsearcher is correctly closed 
   * when the spellchecker is concurrently accessed and closed.
   */
  public void testConcurrentAccess() throws IOException, InterruptedException {
    assertEquals(1, searchers.size());
    final IndexReader r = DirectoryReader.open(userindex);
    spellChecker.clearIndex();
    assertEquals(2, searchers.size());
    addwords(r, spellChecker, "field1");
    assertEquals(3, searchers.size());
    int num_field1 = this.numdoc();
    addwords(r, spellChecker, "field2");
    assertEquals(4, searchers.size());
    int num_field2 = this.numdoc();
    assertEquals(num_field2, num_field1 + 1);
    int numThreads = 5 + random().nextInt(5);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads, new NamedThreadFactory("testConcurrentAccess"));
    SpellCheckWorker[] workers = new SpellCheckWorker[numThreads];
    for (int i = 0; i < numThreads; i++) {
      SpellCheckWorker spellCheckWorker = new SpellCheckWorker(r);
      executor.execute(spellCheckWorker);
      workers[i] = spellCheckWorker;
      
    }
    int iterations = 5 + random().nextInt(5);
    for (int i = 0; i < iterations; i++) {
      Thread.sleep(100);
      // concurrently reset the spell index
      spellChecker.setSpellIndex(this.spellindex);
      // for debug - prints the internal open searchers 
      // showSearchersOpen();
    }
    
    spellChecker.close();
    executor.shutdown();
    // wait for 60 seconds - usually this is very fast but coverage runs could take quite long
    executor.awaitTermination(60L, TimeUnit.SECONDS);
    
    for (int i = 0; i < workers.length; i++) {
      assertFalse(String.format(Locale.ROOT, "worker thread %d failed", i), workers[i].failed);
      assertTrue(String.format(Locale.ROOT, "worker thread %d is still running but should be terminated", i), workers[i].terminated);
    }
    // 4 searchers more than iterations
    // 1. at creation
    // 2. clearIndex()
    // 2. and 3. during addwords
    assertEquals(iterations + 4, searchers.size());
    assertSearchersClosed();
    r.close();
  }
  
  private void assertLastSearcherOpen(int numSearchers) {
    assertEquals(numSearchers, searchers.size());
    IndexSearcher[] searcherArray = searchers.toArray(new IndexSearcher[0]);
    for (int i = 0; i < searcherArray.length; i++) {
      if (i == searcherArray.length - 1) {
        assertTrue("expected last searcher open but was closed",
            searcherArray[i].getIndexReader().getRefCount() > 0);
      } else {
        assertFalse("expected closed searcher but was open - Index: " + i,
            searcherArray[i].getIndexReader().getRefCount() > 0);
      }
    }
  }
  
  private void assertSearchersClosed() {
    for (IndexSearcher searcher : searchers) {
      assertEquals(0, searcher.getIndexReader().getRefCount());
    }
  }

  // For debug
//  private void showSearchersOpen() {
//    int count = 0;
//    for (IndexSearcher searcher : searchers) {
//      if(searcher.getIndexReader().getRefCount() > 0)
//        ++count;
//    } 
//    System.out.println(count);
//  }

  
  private class SpellCheckWorker implements Runnable {
    private final IndexReader reader;
    volatile boolean terminated = false;
    volatile boolean failed = false;
    
    SpellCheckWorker(IndexReader reader) {
      super();
      this.reader = reader;
    }
    
    @Override
    public void run() {
      try {
        while (true) {
          try {
            checkCommonSuggestions(reader);
          } catch (AlreadyClosedException e) {
            
            return;
          } catch (Throwable e) {
            
            e.printStackTrace();
            failed = true;
            return;
          }
        }
      } finally {
        terminated = true;
      }
    }
    
  }
  
  class SpellCheckerMock extends SpellChecker {
    public SpellCheckerMock(Directory spellIndex) throws IOException {
      super(spellIndex);
    }

    public SpellCheckerMock(Directory spellIndex, StringDistance sd)
        throws IOException {
      super(spellIndex, sd);
    }

    public SpellCheckerMock(Directory spellIndex, StringDistance sd, Comparator<SuggestWord> comparator) throws IOException {
      super(spellIndex, sd, comparator);
    }

    @Override
    IndexSearcher createSearcher(Directory dir) throws IOException {
      IndexSearcher searcher = super.createSearcher(dir);
      TestSpellChecker.this.searchers.add(searcher);
      return searcher;
    }
  }
  
}
