package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.cl2o.Cl2oTaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.lru.LruTaxonomyWriterCache;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

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

public class TestConcurrentFacetedIndexing extends FacetTestCase {

  // A No-Op TaxonomyWriterCache which always discards all given categories, and
  // always returns true in put(), to indicate some cache entries were cleared.
  private static TaxonomyWriterCache NO_OP_CACHE = new TaxonomyWriterCache() {
    
    @Override
    public void close() {}
    @Override
    public int get(CategoryPath categoryPath) { return -1; }
    @Override
    public boolean put(CategoryPath categoryPath, int ordinal) { return true; }
    @Override
    public boolean isFull() { return true; }
    @Override
    public void clear() {}
    
  };
  
  static CategoryPath newCategory() {
    Random r = random();
    String l1 = "l1." + r.nextInt(10); // l1.0-l1.9 (10 categories)
    String l2 = "l2." + r.nextInt(30); // l2.0-l2.29 (30 categories)
    String l3 = "l3." + r.nextInt(100); // l3.0-l3.99 (100 categories)
    return new CategoryPath(l1, l2, l3);
  }
  
  static TaxonomyWriterCache newTaxoWriterCache(int ndocs) {
    final double d = random().nextDouble();
    if (d < 0.7) {
      // this is the fastest, yet most memory consuming
      return new Cl2oTaxonomyWriterCache(1024, 0.15f, 3);
    } else if (TEST_NIGHTLY && d > 0.98) {
      // this is the slowest, but tests the writer concurrency when no caching is done.
      // only pick it during NIGHTLY tests, and even then, with very low chances.
      return NO_OP_CACHE;
    } else {
      // this is slower than CL2O, but less memory consuming, and exercises finding categories on disk too.
      return new LruTaxonomyWriterCache(ndocs / 10);
    }
  }
  
  public void testConcurrency() throws Exception {
    final AtomicInteger numDocs = new AtomicInteger(atLeast(10000));
    final Directory indexDir = newDirectory();
    final Directory taxoDir = newDirectory();
    final ConcurrentHashMap<String,String> values = new ConcurrentHashMap<String,String>();
    final IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    final DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE, newTaxoWriterCache(numDocs.get()));
    final Thread[] indexThreads = new Thread[atLeast(4)];

    for (int i = 0; i < indexThreads.length; i++) {
      indexThreads[i] = new Thread() {
        private final FacetFields facetFields = new FacetFields(tw);
        
        @Override
        public void run() {
          Random random = random();
          while (numDocs.decrementAndGet() > 0) {
            try {
              Document doc = new Document();
              int numCats = random.nextInt(3) + 1; // 1-3
              List<CategoryPath> cats = new ArrayList<CategoryPath>(numCats);
              while (numCats-- > 0) {
                CategoryPath cp = newCategory();
                cats.add(cp);
                // add all prefixes to values
                int level = cp.length;
                while (level > 0) {
                  String s = cp.subpath(level).toString('/');
                  values.put(s, s);
                  --level;
                }
              }
              facetFields.addFields(doc, cats);
              iw.addDocument(doc);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }
    
    for (Thread t : indexThreads) t.start();
    for (Thread t : indexThreads) t.join();
    
    DirectoryTaxonomyReader tr = new DirectoryTaxonomyReader(tw);
    assertEquals("mismatch number of categories", values.size() + 1, tr.getSize()); // +1 for root category
    int[] parents = tr.getParallelTaxonomyArrays().parents();
    for (String cat : values.keySet()) {
      CategoryPath cp = new CategoryPath(cat, '/');
      assertTrue("category not found " + cp, tr.getOrdinal(cp) > 0);
      int level = cp.length;
      int parentOrd = 0; // for root, parent is always virtual ROOT (ord=0)
      CategoryPath path = CategoryPath.EMPTY;
      for (int i = 0; i < level; i++) {
        path = cp.subpath(i + 1);
        int ord = tr.getOrdinal(path);
        assertEquals("invalid parent for cp=" + path, parentOrd, parents[ord]);
        parentOrd = ord; // next level should have this parent
      }
    }
    tr.close();

    IOUtils.close(tw, iw, taxoDir, indexDir);
  }

}
