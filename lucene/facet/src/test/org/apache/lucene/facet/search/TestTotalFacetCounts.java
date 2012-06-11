package org.apache.lucene.facet.search;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.FacetTestUtils;
import org.apache.lucene.facet.FacetTestUtils.IndexTaxonomyReaderPair;
import org.apache.lucene.facet.FacetTestUtils.IndexTaxonomyWriterPair;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;

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

public class TestTotalFacetCounts extends LuceneTestCase {

  private static void initCache(int numEntries) {
    TotalFacetCountsCache.getSingleton().clear();
    TotalFacetCountsCache.getSingleton().setCacheSize(numEntries); // Set to keep one in mem
  }

  @Test
  public void testWriteRead() throws IOException {
    doTestWriteRead(14);
    doTestWriteRead(100);
    doTestWriteRead(7);
    doTestWriteRead(3);
    doTestWriteRead(1);
  }

  private void doTestWriteRead(final int partitionSize) throws IOException {
    initCache(1);

    // Create temporary RAMDirectories
    Directory[][] dirs = FacetTestUtils.createIndexTaxonomyDirs(1);
    // Create our index/taxonomy writers
    IndexTaxonomyWriterPair[] writers = FacetTestUtils
    .createIndexTaxonomyWriterPair(dirs);
    DefaultFacetIndexingParams iParams = new DefaultFacetIndexingParams() {
      @Override
      protected int fixedPartitionSize() {
        return partitionSize;
      }
    };
    // The counts that the TotalFacetCountsArray should have after adding
    // the below facets to the index.
    int[] expectedCounts = new int[] { 0, 3, 1, 3, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1 };
    
    // Add a facet to the index
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "a", "b");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "c", "d");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "a", "e");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "a", "d");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "c", "g");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "c", "z");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "b", "a");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "1", "2");
    TestTotalFacetCountsCache.addFacets(iParams, writers[0].indexWriter, writers[0].taxWriter, "b", "c");

    // Commit Changes
    writers[0].commit();
    writers[0].close();

    IndexTaxonomyReaderPair[] readers = 
      FacetTestUtils.createIndexTaxonomyReaderPair(dirs);
    
    int[] intArray = new int[iParams.getPartitionSize()];

    TotalFacetCountsCache tfcc = TotalFacetCountsCache.getSingleton();
    File tmpFile = _TestUtil.createTempFile("test", "tmp", TEMP_DIR);
    tfcc.store(tmpFile, readers[0].indexReader, readers[0].taxReader, iParams, null);
    tfcc.clear(); // not really required because TFCC overrides on load(), but in the test we need not rely on this.
    tfcc.load(tmpFile, readers[0].indexReader, readers[0].taxReader, iParams);
    
    // now retrieve the one just loaded
    TotalFacetCounts totalCounts = 
      tfcc.getTotalCounts(readers[0].indexReader, readers[0].taxReader, iParams, null);

    int partition = 0;
    for (int i=0; i<expectedCounts.length; i+=partitionSize) {
      totalCounts.fillTotalCountsForPartition(intArray, partition);
      int[] partitionExpectedCounts = new int[partitionSize];
      int nToCopy = Math.min(partitionSize,expectedCounts.length-i);
      System.arraycopy(expectedCounts, i, partitionExpectedCounts, 0, nToCopy);
      assertTrue("Wrong counts! for partition "+partition+
          "\nExpected:\n" + Arrays.toString(partitionExpectedCounts)+
          "\nActual:\n" + Arrays.toString(intArray),
          Arrays.equals(partitionExpectedCounts, intArray));
      ++partition;
    }
    readers[0].close();
    IOUtils.close(dirs[0]);
    tmpFile.delete();
  }

}
