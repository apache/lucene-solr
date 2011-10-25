package org.apache.lucene.facet.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.FacetTestUtils;
import org.apache.lucene.facet.FacetTestUtils.IndexTaxonomyReaderPair;
import org.apache.lucene.facet.FacetTestUtils.IndexTaxonomyWriterPair;
import org.apache.lucene.facet.example.ExampleResult;
import org.apache.lucene.facet.example.TestMultiCLExample;
import org.apache.lucene.facet.example.multiCL.MultiCLIndexer;
import org.apache.lucene.facet.example.multiCL.MultiCLSearcher;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.TotalFacetCounts.CreationType;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyReader;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyWriter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SlowRAMDirectory;
import org.apache.lucene.util._TestUtil;

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

public class TestTotalFacetCountsCache extends LuceneTestCase {

  static final TotalFacetCountsCache TFC = TotalFacetCountsCache.getSingleton();

  /**
   * Thread class to be used in tests for this method. This thread gets a TFC
   * and records times.
   */
  private static class TFCThread extends Thread {
    private final IndexReader r;
    private final LuceneTaxonomyReader tr;
    private final FacetIndexingParams iParams;
    
    TotalFacetCounts tfc;

    public TFCThread(IndexReader r, LuceneTaxonomyReader tr, FacetIndexingParams iParams) {
      this.r = r;
      this.tr = tr;
      this.iParams = iParams;
    }
    @Override
    public void run() {
      try {
        tfc = TFC.getTotalCounts(r, tr, iParams, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Utility method to add a document and facets to an index/taxonomy. */
  static void addFacets(FacetIndexingParams iParams, IndexWriter iw,
                        TaxonomyWriter tw, String... strings) throws IOException {
    ArrayList<CategoryPath> cps = new ArrayList<CategoryPath>();
    cps.add(new CategoryPath(strings));
    CategoryDocumentBuilder builder = new CategoryDocumentBuilder(tw, iParams);
    iw.addDocument(builder.setCategoryPaths(cps).build(new Document()));
  }

  /** Clears the cache and sets its size to one. */
  static void initCache() {
    TFC.clear();
    TFC.setCacheSize(1); // Set to keep one in memory
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    initCache();
  }

  /** runs a few instances of {@link MultiCLSearcher} in parallel */
  public void testGeneralSynchronization() throws Exception {
    int numIters = atLeast(2);
    for (int i = 0; i < numIters; i++) {
      doTestGeneralSynchronization(_TestUtil.nextInt(random, 2, 4),
                                  random.nextBoolean() ? -1 : _TestUtil.nextInt(random, 1, 10),
                                  _TestUtil.nextInt(random, 0, 3));
    }
  }

  /**
   * Run many instances of {@link MultiCLSearcher} in parallel, results should
   * be sane. Each instance has a random delay for reading bytes, to ensure
   * that threads finish in different order than started.
   */
  @Test @Nightly
  public void testGeneralSynchronizationBig() throws Exception {
    int[] numThreads = new int[] { 2, 3, 5, 8 };
    int[] sleepMillis = new int[] { -1, 1, 20, 33 };
    int[] cacheSize = new int[] { 0,1,2,3,5 };
    for (int size : cacheSize) {
      for (int sleep : sleepMillis) {
        for (int nThreads : numThreads) {
          doTestGeneralSynchronization(nThreads, sleep, size);
        }
      }
    }
  }

  private void doTestGeneralSynchronization(int numThreads, int sleepMillis,
      int cacheSize) throws Exception, CorruptIndexException, IOException,
      InterruptedException {
    TFC.setCacheSize(cacheSize);
    SlowRAMDirectory slowIndexDir = new SlowRAMDirectory(-1, random);
    MockDirectoryWrapper indexDir = new MockDirectoryWrapper(random, slowIndexDir);
    SlowRAMDirectory slowTaxoDir = new SlowRAMDirectory(-1, random);
    MockDirectoryWrapper taxoDir = new MockDirectoryWrapper(random, slowTaxoDir);
    

    // Index documents without the "slowness"
    MultiCLIndexer.index(indexDir, taxoDir);

    slowIndexDir.setSleepMillis(sleepMillis);
    slowTaxoDir.setSleepMillis(sleepMillis);
    
    // Open the slow readers
    IndexReader slowIndexReader = IndexReader.open(indexDir);
    TaxonomyReader slowTaxoReader = new LuceneTaxonomyReader(taxoDir);

    // Class to perform search and return results as threads
    class Multi extends Thread {
      private List<FacetResult> results;
      private FacetIndexingParams iParams;
      private IndexReader indexReader;
      private TaxonomyReader taxoReader;

      public Multi(IndexReader indexReader, TaxonomyReader taxoReader,
                    FacetIndexingParams iParams) {
        this.indexReader = indexReader;
        this.taxoReader = taxoReader;
        this.iParams = iParams;
      }

      public ExampleResult getResults() {
        ExampleResult exampleRes = new ExampleResult();
        exampleRes.setFacetResults(results);
        return exampleRes;
      }

      @Override
      public void run() {
        try {
          results = MultiCLSearcher.searchWithFacets(indexReader, taxoReader, iParams);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Instantiate threads, but do not start them
    Multi[] multis = new Multi[numThreads];
    for (int i = 0; i < numThreads - 1; i++) {
      multis[i] = new Multi(slowIndexReader, slowTaxoReader, MultiCLIndexer.MULTI_IPARAMS);
    }
    // The last thread uses ONLY the DefaultFacetIndexingParams so that
    // it references a different TFC cache. This will still result
    // in valid results, but will only search one of the category lists
    // instead of all of them.
    multis[numThreads - 1] = new Multi(slowIndexReader, slowTaxoReader, new DefaultFacetIndexingParams());

    // Gentleman, start your engines
    for (Multi m : multis) {
      m.start();
    }

    // Wait for threads and get results
    ExampleResult[] multiResults = new ExampleResult[numThreads];
    for (int i = 0; i < numThreads; i++) {
      multis[i].join();
      multiResults[i] = multis[i].getResults();
    }

    // Each of the (numThreads-1) should have the same predictable
    // results, which we test for here.
    for (int i = 0; i < numThreads - 1; i++) {
      ExampleResult eResults = multiResults[i];
      TestMultiCLExample.assertCorrectMultiResults(eResults);
    }

    // The last thread, which only searched over the
    // DefaultFacetIndexingParams,
    // has its own results
    ExampleResult eResults = multiResults[numThreads - 1];
    List<FacetResult> results = eResults.getFacetResults();
    assertEquals(3, results.size());
    String[] expLabels = new String[] { "5", "5/5", "6/2" };
    double[] expValues = new double[] { 0.0, 0.0, 1.0 };
    for (int i = 0; i < 3; i++) {
      FacetResult result = results.get(i);
      assertNotNull("Result should not be null", result);
      FacetResultNode resNode = result.getFacetResultNode();
      assertEquals("Invalid label", expLabels[i], resNode.getLabel().toString());
      assertEquals("Invalid value", expValues[i], resNode.getValue(), 0.0);
      assertEquals("Invalid number of subresults", 0, resNode.getNumSubResults());
    }
    // we're done, close the index reader and the taxonomy.
    slowIndexReader.close();
    slowTaxoReader.close();
    indexDir.close();
    taxoDir.close();
  }

  /**
   * Simple test to make sure the TotalFacetCountsManager updates the
   * TotalFacetCounts array only when it is supposed to, and whether it
   * is recomputed or read from disk.
   */
  @Test
  public void testGenerationalConsistency() throws Exception {
    // Create temporary RAMDirectories
    Directory[][] dirs = FacetTestUtils.createIndexTaxonomyDirs(1);

    // Create our index/taxonomy writers
    IndexTaxonomyWriterPair[] writers = FacetTestUtils.createIndexTaxonomyWriterPair(dirs);
    DefaultFacetIndexingParams iParams = new DefaultFacetIndexingParams();

    // Add a facet to the index
    addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "a", "b");

    // Commit Changes
    writers[0].indexWriter.commit();
    writers[0].taxWriter.commit();

    // Open readers
    IndexTaxonomyReaderPair[] readers = FacetTestUtils.createIndexTaxonomyReaderPair(dirs);

    // As this is the first time we have invoked the TotalFacetCountsManager, 
    // we should expect to compute and not read from disk.
    TotalFacetCounts totalCounts = 
      TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    int prevGen = assertRecomputed(totalCounts, 0, "after first attempt to get it!");

    // Repeating same operation should pull from the cache - not recomputed. 
    assertTrue("Should be obtained from cache at 2nd attempt",totalCounts == 
      TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null));

    // Repeat the same operation as above. but clear first - now should recompute again
    initCache();
    totalCounts = TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts, prevGen, "after cache clear, 3rd attempt to get it!");
    
    //store to file
    File outputFile = _TestUtil.createTempFile("test", "tmp", TEMP_DIR);
    initCache();
    TFC.store(outputFile, readers[0].indexReader, readers[0].taxReader, iParams, null);
    totalCounts = TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts, prevGen, "after cache clear, 4th attempt to get it!");

    //clear and load
    initCache();
    TFC.load(outputFile, readers[0].indexReader, readers[0].taxReader, iParams);
    totalCounts = TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertReadFromDisc(totalCounts, prevGen, "after 5th attempt to get it!");

    // Add a new facet to the index, commit and refresh readers
    addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "c", "d");
    writers[0].indexWriter.close();
    writers[0].taxWriter.close();

    readers[0].taxReader.refresh();
    IndexReader r2 = IndexReader.openIfChanged(readers[0].indexReader);
    assertNotNull(r2);
    // Hold on to the 'original' reader so we can do some checks with it
    IndexReader origReader = null;

    assertTrue("Reader must be updated!", readers[0].indexReader != r2);
    
    // Set the 'original' reader
    origReader = readers[0].indexReader;
    // Set the new master index Reader
    readers[0].indexReader = r2;

    // Try to get total-counts the originalReader AGAIN, just for sanity. Should pull from the cache - not recomputed. 
    assertTrue("Should be obtained from cache at 6th attempt",totalCounts == 
      TFC.getTotalCounts(origReader, readers[0].taxReader, iParams, null));

    // now use the new reader - should recompute
    totalCounts = TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts, prevGen, "after updating the index - 7th attempt!");

    // try again - should not recompute
    assertTrue("Should be obtained from cache at 8th attempt",totalCounts == 
      TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null));
    
    // delete a doc from the reader and commit - should recompute
    origReader.close();
    origReader = readers[0].indexReader;
    readers[0].indexReader = IndexReader.open(origReader.directory(),false);
    initCache();
    totalCounts = TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts, prevGen, "after opening a writable reader - 9th attempt!");
    // now do the delete
    readers[0].indexReader.deleteDocument(1);
    readers[0].indexReader.commit(null);
    totalCounts = TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts, prevGen, "after deleting docs the index - 10th attempt!");
    
    origReader.close();
    readers[0].close();
    r2.close();
    outputFile.delete();
    IOUtils.close(dirs[0]);
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
    // Create temporary RAMDirectories
    Directory[][] dirs = FacetTestUtils.createIndexTaxonomyDirs(1);
    // Create our index/taxonomy writers
    IndexTaxonomyWriterPair[] writers = FacetTestUtils
    .createIndexTaxonomyWriterPair(dirs);
    DefaultFacetIndexingParams iParams = new DefaultFacetIndexingParams() {
      @Override
      protected int fixedPartitionSize() {
        return 2;
      }
    };
    // Add a facet to the index
    addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "a", "b");
    // Commit Changes
    writers[0].indexWriter.commit();
    writers[0].taxWriter.commit();

    IndexTaxonomyReaderPair[] readers = FacetTestUtils.createIndexTaxonomyReaderPair(dirs);

    // Create TFC and write cache to disk
    File outputFile = _TestUtil.createTempFile("test", "tmp", TEMP_DIR);
    TFC.store(outputFile, readers[0].indexReader, readers[0].taxReader, iParams, null);
    
    // Make the taxonomy grow without touching the index
    for (int i = 0; i < 10; i++) {
      writers[0].taxWriter.addCategory(new CategoryPath("foo", Integer.toString(i)));
    }
    writers[0].taxWriter.commit();
    readers[0].taxReader.refresh();

    initCache();

    // With the bug, this next call should result in an exception
    TFC.load(outputFile, readers[0].indexReader, readers[0].taxReader, iParams);
    TotalFacetCounts totalCounts = TFC.getTotalCounts(
        readers[0].indexReader, readers[0].taxReader, iParams, null);
    assertReadFromDisc(totalCounts, 0, "after reading from disk.");
    outputFile.delete();
    writers[0].close();
    readers[0].close();
    IOUtils.close(dirs[0]);
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
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
    LuceneTaxonomyWriter tw = new LuceneTaxonomyWriter(taxoDir);
    DefaultFacetIndexingParams iParams = new DefaultFacetIndexingParams();
    // Add documents and facets
    for (int i = 0; i < 1000; i++) {
      addFacets(iParams, w, tw, "facet", Integer.toString(i));
    }
    w.close();
    tw.close();

    indexDir.setSleepMillis(1);
    taxoDir.setSleepMillis(1);

    IndexReader r = IndexReader.open(indexDir);
    LuceneTaxonomyReader tr = new LuceneTaxonomyReader(taxoDir);

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
    // Create temporary RAMDirectories
    Directory[][] dirs = FacetTestUtils.createIndexTaxonomyDirs(2);
    // Create our index/taxonomy writers
    IndexTaxonomyWriterPair[] writers = FacetTestUtils.createIndexTaxonomyWriterPair(dirs);
    DefaultFacetIndexingParams iParams = new DefaultFacetIndexingParams();

    // Add a facet to the index
    addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "a", "b");
    addFacets(iParams, writers[1].indexWriter, writers[1].taxWriter, "d", "e");
    // Commit Changes
    writers[0].indexWriter.commit();
    writers[0].taxWriter.commit();
    writers[1].indexWriter.commit();
    writers[1].taxWriter.commit();

    // Open two readers
    IndexTaxonomyReaderPair[] readers = FacetTestUtils.createIndexTaxonomyReaderPair(dirs);

    // As this is the first time we have invoked the TotalFacetCountsManager, we
    // should expect to compute.
    TotalFacetCounts totalCounts0 = 
      TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    int prevGen = -1;
    prevGen = assertRecomputed(totalCounts0, prevGen, "after attempt 1");
    assertTrue("attempt 1b for same input [0] shout find it in cache",
        totalCounts0 == TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null));
    
    // 2nd Reader - As this is the first time we have invoked the
    // TotalFacetCountsManager, we should expect a state of NEW to be returned.
    TotalFacetCounts totalCounts1 = 
      TFC.getTotalCounts(readers[1].indexReader, readers[1].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts1, prevGen, "after attempt 2");
    assertTrue("attempt 2b for same input [1] shout find it in cache",
        totalCounts1 == TFC.getTotalCounts(readers[1].indexReader, readers[1].taxReader, iParams, null));

    // Right now cache size is one, so first TFC is gone and should be recomputed  
    totalCounts0 = 
      TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts0, prevGen, "after attempt 3");
    
    // Similarly will recompute the second result  
    totalCounts1 = 
      TFC.getTotalCounts(readers[1].indexReader, readers[1].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts1, prevGen, "after attempt 4");

    // Now we set the cache size to two, meaning both should exist in the
    // cache simultaneously
    TFC.setCacheSize(2);

    // Re-compute totalCounts0 (was evicted from the cache when the cache was smaller)
    totalCounts0 = 
      TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);
    prevGen = assertRecomputed(totalCounts0, prevGen, "after attempt 5");

    // now both are in the larger cache and should not be recomputed 
    totalCounts1 = TFC.getTotalCounts(readers[1].indexReader,
        readers[1].taxReader, iParams, null);
    assertTrue("with cache of size 2 res no. 0 should come from cache",
        totalCounts0 == TFC.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null));
    assertTrue("with cache of size 2 res no. 1 should come from cache",
        totalCounts1 == TFC.getTotalCounts(readers[1].indexReader, readers[1].taxReader, iParams, null));
    
    writers[0].close();
    writers[1].close();
    readers[0].close();
    readers[1].close();
    for (Directory[] dirset : dirs) {
      IOUtils.close(dirset);
    }
  }

}
