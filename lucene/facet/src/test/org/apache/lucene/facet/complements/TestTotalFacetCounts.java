package org.apache.lucene.facet.complements;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.complements.TotalFacetCounts;
import org.apache.lucene.facet.complements.TotalFacetCountsCache;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;
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

public class TestTotalFacetCounts extends FacetTestCase {

  private static void initCache() {
    TotalFacetCountsCache.getSingleton().clear();
    TotalFacetCountsCache.getSingleton().setCacheSize(1); // Set to keep one in mem
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
    initCache();

    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriter indexWriter = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    
    FacetIndexingParams iParams = new FacetIndexingParams() {
      @Override
      public int getPartitionSize() {
        return partitionSize;
      }
      
      @Override
      public CategoryListParams getCategoryListParams(CategoryPath category) {
        return new CategoryListParams() {
          @Override
          public OrdinalPolicy getOrdinalPolicy(String dimension) {
            return OrdinalPolicy.ALL_PARENTS;
          }
        };
      }
    };
    // The counts that the TotalFacetCountsArray should have after adding
    // the below facets to the index.
    int[] expectedCounts = new int[] { 0, 3, 1, 3, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1 };
    String[] categories = new String[] { "a/b", "c/d", "a/e", "a/d", "c/g", "c/z", "b/a", "1/2", "b/c" };

    FacetFields facetFields = new FacetFields(taxoWriter, iParams);
    for (String cat : categories) {
      Document doc = new Document();
      facetFields.addFields(doc, Collections.singletonList(new CategoryPath(cat, '/')));
      indexWriter.addDocument(doc);
    }

    // Commit Changes
    IOUtils.close(indexWriter, taxoWriter);

    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    
    int[] intArray = new int[iParams.getPartitionSize()];

    TotalFacetCountsCache tfcc = TotalFacetCountsCache.getSingleton();
    File tmpFile = _TestUtil.createTempFile("test", "tmp", TEMP_DIR);
    tfcc.store(tmpFile, indexReader, taxoReader, iParams);
    tfcc.clear(); // not really required because TFCC overrides on load(), but in the test we need not rely on this.
    tfcc.load(tmpFile, indexReader, taxoReader, iParams);
    
    // now retrieve the one just loaded
    TotalFacetCounts totalCounts = tfcc.getTotalCounts(indexReader, taxoReader, iParams);

    int partition = 0;
    for (int i = 0; i < expectedCounts.length; i += partitionSize) {
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
    IOUtils.close(indexReader, taxoReader);
    IOUtils.close(indexDir, taxoDir);
    tmpFile.delete();
  }

}
