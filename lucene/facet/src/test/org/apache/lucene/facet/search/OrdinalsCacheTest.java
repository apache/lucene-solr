package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
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

public class OrdinalsCacheTest extends FacetTestCase {

  @Test
  public void testOrdinalsCacheWithThreads() throws Exception {
    // LUCENE-5303: OrdinalsCache used the ThreadLocal BinaryDV instead of reader.getCoreCacheKey().
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(indexDir, conf);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetFields facetFields = new FacetFields(taxoWriter);
    
    Document doc = new Document();
    facetFields.addFields(doc, Arrays.asList(new CategoryPath("A", "1")));
    writer.addDocument(doc);
    doc = new Document();
    facetFields.addFields(doc, Arrays.asList(new CategoryPath("A", "2")));
    writer.addDocument(doc);
    writer.close();
    taxoWriter.close();
    
    final DirectoryReader reader = DirectoryReader.open(indexDir);
    Thread[] threads = new Thread[3];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread("CachedOrdsThread-" + i) {
        @Override
        public void run() {
          for (AtomicReaderContext context : reader.leaves()) {
            try {
              OrdinalsCache.getCachedOrds(context, FacetIndexingParams.DEFAULT.getCategoryListParams(new CategoryPath("A")));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    OrdinalsCache.clear();

    long ramBytesUsed = 0;
    for (Thread t : threads) {
      t.start();
      t.join();
      if (ramBytesUsed == 0) {
        ramBytesUsed = OrdinalsCache.ramBytesUsed();
      } else {
        assertEquals(ramBytesUsed, OrdinalsCache.ramBytesUsed());
      }
    }
    
    reader.close();
    
    IOUtils.close(indexDir, taxoDir);
  }
  
}
