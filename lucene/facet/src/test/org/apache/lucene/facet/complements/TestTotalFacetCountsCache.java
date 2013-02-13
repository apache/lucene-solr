package org.apache.lucene.facet.complements;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.SlowRAMDirectory;
import org.apache.lucene.facet.complements.TotalFacetCounts;
import org.apache.lucene.facet.complements.TotalFacetCountsCache;
import org.apache.lucene.facet.complements.TotalFacetCounts.CreationType;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;
import org.junit.Before;
import org.junit.Test;

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

public class TestTotalFacetCountsCache extends FacetTestCase {

  static final TotalFacetCountsCache TFC = TotalFacetCountsCache.getSingleton();

  /**
   * Thread class to be used in tests for this method. This thread gets a TFC
   * and records times.
   */
  private static class TFCThread extends Thread {
    private final IndexReader r;
    private final DirectoryTaxonomyReader tr;
    private final FacetIndexingParams iParams;
    
    TotalFacetCounts tfc;

    public TFCThread(IndexReader r, DirectoryTaxonomyReader tr, FacetIndexingParams iParams) {
      this.r = r;
      this.tr = tr;
      this.iParams = iParams;
    }
    @Override
    public void run() {
      try {
        tfc = TFC.getTotalCounts(r, tr, iParams);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Utility method to add a document and facets to an index/taxonomy. */
  private static void addFacets(FacetIndexingParams iParams, IndexWriter iw,
      TaxonomyWriter tw, String... strings) throws IOException {
    Document doc = new Document();
    FacetFields facetFields = new FacetFields(tw, iParams);
    facetFields.addFields(doc, Collections.singletonList(new CategoryPath(strings)));
    iw.addDocument(doc);
  }

  /** Clears the cache and sets its size to one. */
  private static void initCache() {
    TFC.clear();
    TFC.setCacheSize(1); // Set to keep one in memory
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    initCache();
  }

  /** runs few searches in parallel */
  public void testGeneralSynchronization() throws Exception {
    int numIters = atLeast(4);
    Random random = random();
    for (int i = 0; i < numIters; i++) {
      int numThreads = random.nextInt(3) + 2; // 2-4
      int sleepMillis = random.nextBoolean() ? -1 : random.nextInt(10) + 1 /*1-10*/;
      int cacheSize = random.nextInt(4); // 0-3
      doTestGeneralSynchronization(numThreads, sleepMillis, cacheSize);
    }
  }

  private static final String[] CATEGORIES = new String[] { "a/b", "c/d", "a/e", "a/d", "c/g", "c/z", "b/a", "1/2", "b/c" };

  private void index(Directory indexDir, Directory taxoDir) throws IOException {
    IndexWriter indexWriter = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetFields facetFields = new FacetFields(taxoWriter);
    
    for (String cat : CATEGORIES) {
      Document doc = new Document();
      facetFields.addFields(doc, Collections.singletonList(new CategoryPath(cat, '/')));
      indexWriter.addDocument(doc);
    }
    
    IOUtils.close(indexWriter, taxoWriter);
  }
  
  private void doTestGeneralSynchronization(int numThreads, int sleepMillis, int cacheSize) throws Exception {
    TFC.setCacheSize(cacheSize);
    SlowRAMDirectory slowIndexDir = new SlowRAMDirectory(-1, random());
    MockDirectoryWrapper indexDir = new MockDirectoryWrapper(random(), slowIndexDir);
    SlowRAMDirectory slowTaxoDir = new SlowRAMDirectory(-1, random());
    MockDirectoryWrapper taxoDir = new MockDirectoryWrapper(random(), slowTaxoDir);

    // Index documents without the "slowness"
    index(indexDir, taxoDir);

    slowIndexDir.setSleepMillis(sleepMillis);
    slowTaxoDir.setSleepMillis(sleepMillis);
    
    // Open the slow readers
    IndexReader slowIndexReader = DirectoryReader.open(indexDir);
    TaxonomyReader slowTaxoReader = new DirectoryTaxonomyReader(taxoDir);

    // Class to perform search and return results as threads
    class Multi extends Thread {
      private List<FacetResult> results;
      private FacetIndexingParams iParams;
      private IndexReader indexReader;
      private TaxonomyReader taxoReader;

      public Multi(IndexReader indexReader, TaxonomyReader taxoReader, FacetIndexingParams iParams) {
        this.indexReader = indexReader;
        this.taxoReader = taxoReader;
        this.iParams = iParams;
      }

      public List<FacetResult> getResults() {
        return results;
      }

      @Override
      public void run() {
        try {
          FacetSearchParams fsp = new FacetSearchParams(iParams, new CountFacetRequest(new CategoryPath("a"), 10),
              new CountFacetRequest(new CategoryPath("b"), 10));
          IndexSearcher searcher = new IndexSearcher(indexReader);
          FacetsCollector fc = FacetsCollector.create(fsp, indexReader, taxoReader);
          searcher.search(new MatchAllDocsQuery(), fc);
          results = fc.getFacetResults();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    Multi[] multis = new Multi[numThreads];
    for (int i = 0; i < numThreads; i++) {
      multis[i] = new Multi(slowIndexReader, slowTaxoReader, FacetIndexingParams.DEFAULT);
    }

    for (Multi m : multis) {
      m.start();
    }

    // Wait for threads and get results
    String[] expLabelsA = new String[] { "a/d", "a/e", "a/b" };
    String[] expLabelsB = new String[] { "b/c", "b/a" };
    for (Multi m : multis) {
      m.join();
      List<FacetResult> facetResults = m.getResults();
      assertEquals("expected two results", 2, facetResults.size());
      
      FacetResultNode nodeA = facetResults.get(0).getFacetResultNode();
      int i = 0;
      for (FacetResultNode node : nodeA.subResults) {
        assertEquals("wrong count", 1, (int) node.value);
        assertEquals(expLabelsA[i++], node.label.toString('/'));
      }
      
      FacetResultNode nodeB = facetResults.get(1).getFacetResultNode();
      i = 0;
      for (FacetResultNode node : nodeB.subResults) {
        assertEquals("wrong count", 1, (int) node.value);
        assertEquals(expLabelsB[i++], node.label.toString('/'));
      }
    }
    
    IOUtils.close(slowIndexReader, slowTaxoReader, indexDir, taxoDir);
  }

  /**
   * Simple test to make sure the TotalFacetCountsManager updates the
   * TotalFacetCounts array only when it is supposed to, and whether it
   * is recomputed or read from disk.
   */
  @Test
  public void testGenerationalConsistency() throws Exception {
    // Create temporary RAMDirectories
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    // Create our index/taxonomy writers
    IndexWriter indexWriter = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetIndexingParams iParams = FacetIndexingParams.DEFAULT;

    // Add a facet to the index
    addFacets(iParams, indexWriter, taxoWriter, "a", "b");

    // Commit Changes
    indexWriter.commit();
    taxoWriter.commit();

    // Open readers
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    // As this is the first time we have invoked the TotalFacetCountsManager, 
    // we should expect to compute and not read from disk.
    TotalFacetCounts totalCounts = TFC.getTotalCounts(indexReader, taxoReader, iParams);
    int prevGen = assertRecomputed(totalCounts, 0, "after first attempt to get it!");

    // Repeating same operation should pull from the cache - not recomputed. 
    assertTrue("Should be obtained from cache at 2nd attempt",totalCounts == 
      TFC.getTotalCounts(indexReader, taxoReader, iParams));

    // Repeat the same operation as above. but clear first - now should recompute again
    initCache();
    totalCounts = TFC.getTotalCounts(indexReader, taxoReader, iParams);
    prevGen = assertRecomputed(totalCounts, prevGen, "after cache clear, 3rd attempt to get it!");
    
    //store to file
    File outputFile = _TestUtil.createTempFile("test", "tmp", TEMP_DIR);
    initCache();
    TFC.store(outputFile, indexReader, taxoReader, iParams);
    totalCounts = TFC.getTotalCounts(indexReader, taxoReader, iParams);
    prevGen = assertRecomputed(totalCounts, prevGen, "after cache clear, 4th attempt to get it!");

    //clear and load
    initCache();
    TFC.load(outputFile, indexReader, taxoReader, iParams);
    totalCounts = TFC.getTotalCounts(indexReader, taxoReader, iParams);
    prevGen = assertReadFromDisc(totalCounts, prevGen, "after 5th attempt to get it!");

    // Add a new facet to the index, commit and refresh readers
    addFacets(iParams, indexWriter, taxoWriter, "c", "d");
    IOUtils.close(indexWriter, taxoWriter);

    TaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(taxoReader);
    assertNotNull(newTaxoReader);
    assertTrue("should have received more cagtegories in updated taxonomy", newTaxoReader.getSize() > taxoReader.getSize());
    taxoReader.close();
    taxoReader = newTaxoReader;
    
    DirectoryReader r2 = DirectoryReader.openIfChanged(indexReader);
    assertNotNull(r2);
    indexReader.close();
    indexReader = r2;

    // now use the new reader - should recompute
    totalCounts = TFC.getTotalCounts(indexReader, taxoReader, iParams);
    prevGen = assertRecomputed(totalCounts, prevGen, "after updating the index - 7th attempt!");

    // try again - should not recompute
    assertTrue("Should be obtained from cache at 8th attempt",totalCounts == 
      TFC.getTotalCounts(indexReader, taxoReader, iParams));
    
    IOUtils.close(indexReader, taxoReader);
    outputFile.delete();
    IOUtils.close(indexDir, taxoDir);
  }

  private int assertReadFromDisc(TotalFacetCounts totalCounts, int prevGen, String errMsg) {
    assertEquals("should read from disk "+errMsg, CreationType.Loaded, totalCounts.createType4test);
    int gen4test = totalCounts.gen4test;
    assertTrue("should read from disk "+errMsg, gen4test > prevGen);
    return gen4test;
  }
  
  private int assertRecomputed(TotalFacetCounts totalCounts, int prevGen, String errMsg) {
    assertEquals("should recompute "+errMsg, CreationType.Computed, totalCounts.createType4test);
    int gen4test = totalCounts.gen4test;
    assertTrue("should recompute "+errMsg, gen4test > prevGen);
    return gen4test;
  }

  /**
   * This test is to address a bug in a previous version.  If a TFC cache is
   * written to disk, and then the taxonomy grows (but the index does not change),
   * and then the TFC cache is re-read from disk, there will be an exception
   * thrown, as the integers are read off of the disk according to taxonomy
   * size, which has changed.
   */
  @Test
  public void testGrowingTaxonomy() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    // Create our index/taxonomy writers
    IndexWriter indexWriter = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetIndexingParams iParams = new FacetIndexingParams() {
      @Override
      public int getPartitionSize() {
        return 2;
      }
    };
    // Add a facet to the index
    addFacets(iParams, indexWriter, taxoWriter, "a", "b");
    // Commit Changes
    indexWriter.commit();
    taxoWriter.commit();

    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    // Create TFC and write cache to disk
    File outputFile = _TestUtil.createTempFile("test", "tmp", TEMP_DIR);
    TFC.store(outputFile, indexReader, taxoReader, iParams);
    
    // Make the taxonomy grow without touching the index
    for (int i = 0; i < 10; i++) {
      taxoWriter.addCategory(new CategoryPath("foo", Integer.toString(i)));
    }
    taxoWriter.commit();
    TaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(taxoReader);
    assertNotNull(newTaxoReader);
    taxoReader.close();
    taxoReader = newTaxoReader;

    initCache();

    // With the bug, this next call should result in an exception
    TFC.load(outputFile, indexReader, taxoReader, iParams);
    TotalFacetCounts totalCounts = TFC.getTotalCounts(indexReader, taxoReader, iParams);
    assertReadFromDisc(totalCounts, 0, "after reading from disk.");
    
    outputFile.delete();
    IOUtils.close(indexWriter, taxoWriter, indexReader, taxoReader);
    IOUtils.close(indexDir, taxoDir);
  }

  /**
   * Test that a new TFC is only calculated and placed in memory (by two
   * threads who want it at the same time) only once.
   */
  @Test
  public void testMemoryCacheSynchronization() throws Exception {
    SlowRAMDirectory indexDir = new SlowRAMDirectory(-1, null);
    SlowRAMDirectory taxoDir = new SlowRAMDirectory(-1, null);

    // Write index using 'normal' directories
    IndexWriter w = new IndexWriter(indexDir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir);
    FacetIndexingParams iParams = FacetIndexingParams.DEFAULT;
    // Add documents and facets
    for (int i = 0; i < 1000; i++) {
      addFacets(iParams, w, tw, "facet", Integer.toString(i));
    }
    w.close();
    tw.close();

    indexDir.setSleepMillis(1);
    taxoDir.setSleepMillis(1);

    IndexReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader tr = new DirectoryTaxonomyReader(taxoDir);

    // Create and start threads. Thread1 should lock the cache and calculate
    // the TFC array. The second thread should block until the first is
    // done, then successfully retrieve from the cache without recalculating
    // or reading from disk.
    TFCThread tfcCalc1 = new TFCThread(r, tr, iParams);
    TFCThread tfcCalc2 = new TFCThread(r, tr, iParams);
    tfcCalc1.start();
    // Give thread 1 a head start to ensure correct sequencing for testing
    Thread.sleep(5);
    tfcCalc2.start();

    tfcCalc1.join();
    tfcCalc2.join();

    // Since this test ends up with references to the same TFC object, we
    // can only test the times to make sure that they are the same.
    assertRecomputed(tfcCalc1.tfc, 0, "thread 1 should recompute");
    assertRecomputed(tfcCalc2.tfc, 0, "thread 2 should recompute");
    assertTrue("Both results should be the same (as their inputs are the same objects)",
        tfcCalc1.tfc == tfcCalc2.tfc);

    r.close();
    tr.close();
  }

  /**
   * Simple test to make sure the TotalFacetCountsManager updates the
   * TotalFacetCounts array only when it is supposed to, and whether it
   * is recomputed or read from disk, but this time with TWO different
   * TotalFacetCounts
   */
  @Test
  public void testMultipleIndices() throws IOException {
    Directory indexDir1 = newDirectory(), indexDir2 = newDirectory();
    Directory taxoDir1 = newDirectory(), taxoDir2 = newDirectory();
    
    // Create our index/taxonomy writers
    IndexWriter indexWriter1 = new IndexWriter(indexDir1, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    IndexWriter indexWriter2 = new IndexWriter(indexDir2, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    TaxonomyWriter taxoWriter1 = new DirectoryTaxonomyWriter(taxoDir1);
    TaxonomyWriter taxoWriter2 = new DirectoryTaxonomyWriter(taxoDir2);
    FacetIndexingParams iParams = FacetIndexingParams.DEFAULT;

    // Add a facet to the index
    addFacets(iParams, indexWriter1, taxoWriter1, "a", "b");
    addFacets(iParams, indexWriter1, taxoWriter1, "d", "e");
    // Commit Changes
    indexWriter1.commit();
    indexWriter2.commit();
    taxoWriter1.commit();
    taxoWriter2.commit();

    // Open two readers
    DirectoryReader indexReader1 = DirectoryReader.open(indexDir1);
    DirectoryReader indexReader2 = DirectoryReader.open(indexDir2);
    TaxonomyReader taxoReader1 = new DirectoryTaxonomyReader(taxoDir1);
    TaxonomyReader taxoReader2 = new DirectoryTaxonomyReader(taxoDir2);

    // As this is the first time we have invoked the TotalFacetCountsManager, we
    // should expect to compute.
    TotalFacetCounts totalCounts0 = TFC.getTotalCounts(indexReader1, taxoReader1, iParams);
    int prevGen = -1;
    prevGen = assertRecomputed(totalCounts0, prevGen, "after attempt 1");
    assertTrue("attempt 1b for same input [0] shout find it in cache",
        totalCounts0 == TFC.getTotalCounts(indexReader1, taxoReader1, iParams));
    
    // 2nd Reader - As this is the first time we have invoked the
    // TotalFacetCountsManager, we should expect a state of NEW to be returned.
    TotalFacetCounts totalCounts1 = TFC.getTotalCounts(indexReader2, taxoReader2, iParams);
    prevGen = assertRecomputed(totalCounts1, prevGen, "after attempt 2");
    assertTrue("attempt 2b for same input [1] shout find it in cache",
        totalCounts1 == TFC.getTotalCounts(indexReader2, taxoReader2, iParams));

    // Right now cache size is one, so first TFC is gone and should be recomputed  
    totalCounts0 = TFC.getTotalCounts(indexReader1, taxoReader1, iParams);
    prevGen = assertRecomputed(totalCounts0, prevGen, "after attempt 3");
    
    // Similarly will recompute the second result  
    totalCounts1 = TFC.getTotalCounts(indexReader2, taxoReader2, iParams);
    prevGen = assertRecomputed(totalCounts1, prevGen, "after attempt 4");

    // Now we set the cache size to two, meaning both should exist in the
    // cache simultaneously
    TFC.setCacheSize(2);

    // Re-compute totalCounts0 (was evicted from the cache when the cache was smaller)
    totalCounts0 = TFC.getTotalCounts(indexReader1, taxoReader1, iParams);
    prevGen = assertRecomputed(totalCounts0, prevGen, "after attempt 5");

    // now both are in the larger cache and should not be recomputed 
    totalCounts1 = TFC.getTotalCounts(indexReader2, taxoReader2, iParams);
    assertTrue("with cache of size 2 res no. 0 should come from cache",
        totalCounts0 == TFC.getTotalCounts(indexReader1, taxoReader1, iParams));
    assertTrue("with cache of size 2 res no. 1 should come from cache",
        totalCounts1 == TFC.getTotalCounts(indexReader2, taxoReader2, iParams));
    
    IOUtils.close(indexWriter1, indexWriter2, taxoWriter1, taxoWriter2);
    IOUtils.close(indexReader1, indexReader2, taxoReader1, taxoReader2);
    IOUtils.close(indexDir1, indexDir2, taxoDir1, taxoDir2);
  }

}
