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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class TestCachedOrdinalsReader extends FacetTestCase {

  @Test
  public void testWithThreads() throws Exception {
    // LUCENE-5303: OrdinalsCache used the ThreadLocal BinaryDV instead of reader.getCoreCacheKey().
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(indexDir, conf);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    
    Document doc = new Document();
    doc.add(new FacetField("A", "1"));
    writer.addDocument(config.build(taxoWriter, doc));
    doc = new Document();
    doc.add(new FacetField("A", "2"));
    writer.addDocument(config.build(taxoWriter, doc));
    
    final DirectoryReader reader = DirectoryReader.open(writer);
    final CachedOrdinalsReader ordsReader = new CachedOrdinalsReader(new DocValuesOrdinalsReader(FacetsConfig.DEFAULT_INDEX_FIELD_NAME));
    Thread[] threads = new Thread[3];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread("CachedOrdsThread-" + i) {
        @Override
        public void run() {
          for (LeafReaderContext context : reader.leaves()) {
            try {
              ordsReader.getReader(context);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    long ramBytesUsed = 0;
    for (Thread t : threads) {
      t.start();
      t.join();
      if (ramBytesUsed == 0) {
        ramBytesUsed = ordsReader.ramBytesUsed();
      } else {
        assertEquals(ramBytesUsed, ordsReader.ramBytesUsed());
      }
    }

    writer.close();
    IOUtils.close(taxoWriter, reader, indexDir, taxoDir);
  }
}
