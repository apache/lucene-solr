package org.apache.lucene.facet.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;

public class TestSearcherTaxonomyManager extends FacetTestCase {
  public void test() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    final DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir);
    final FacetFields facetFields = new FacetFields(tw);
    final AtomicBoolean stop = new AtomicBoolean();

    // How many unique facets to index before stopping:
    final int ordLimit = TEST_NIGHTLY ? 100000 : 6000;

    Thread indexer = new Thread() {
        @Override
        public void run() {
          try {
            Set<String> seen = new HashSet<String>();
            List<String> paths = new ArrayList<String>();
            while (true) {
              Document doc = new Document();
              List<CategoryPath> docPaths = new ArrayList<CategoryPath>();
              int numPaths = _TestUtil.nextInt(random(), 1, 5);
              for(int i=0;i<numPaths;i++) {
                String path;
                if (!paths.isEmpty() && random().nextInt(5) != 4) {
                  // Use previous path
                  path = paths.get(random().nextInt(paths.size()));
                } else {
                  // Create new path
                  path = null;
                  while (true) {
                    path = _TestUtil.randomRealisticUnicodeString(random());
                    if (path.length() != 0 && !seen.contains(path) && path.indexOf(FacetIndexingParams.DEFAULT_FACET_DELIM_CHAR) == -1) {
                      seen.add(path);
                      paths.add(path);
                      break;
                    }
                  }
                }
                docPaths.add(new CategoryPath("field", path));
              }
              try {
                facetFields.addFields(doc, docPaths);
                w.addDocument(doc);
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }

              if (tw.getSize() >= ordLimit) {
                break;
              }
            }
          } finally {
            stop.set(true);
          }
        }
      };

    final SearcherTaxonomyManager mgr = new SearcherTaxonomyManager(w, true, null, tw);

    Thread reopener = new Thread() {
        @Override
        public void run() {
          while(!stop.get()) {
            try {
              // Sleep for up to 20 msec:
              Thread.sleep(random().nextInt(20));

              if (VERBOSE) {
                System.out.println("TEST: reopen");
              }

              mgr.maybeRefresh();

              if (VERBOSE) {
                System.out.println("TEST: reopen done");
              }
            } catch (Exception ioe) {
              throw new RuntimeException(ioe);
            }
          }
        }
      };
    reopener.start();

    indexer.start();

    try {
      while (!stop.get()) {
        SearcherAndTaxonomy pair = mgr.acquire();
        try {
          //System.out.println("search maxOrd=" + pair.taxonomyReader.getSize());
          int topN = _TestUtil.nextInt(random(), 1, 20);
          CountFacetRequest cfr = new CountFacetRequest(new CategoryPath("field"), topN);
          FacetSearchParams fsp = new FacetSearchParams(cfr);
          FacetsCollector fc = FacetsCollector.create(fsp, pair.searcher.getIndexReader(), pair.taxonomyReader);
          pair.searcher.search(new MatchAllDocsQuery(), fc);
          List<FacetResult> results = fc.getFacetResults();
          FacetResult fr = results.get(0);
          FacetResultNode root = results.get(0).getFacetResultNode();
          assertTrue(root.ordinal != 0);

          if (pair.searcher.getIndexReader().numDocs() > 0) { 
            //System.out.println(pair.taxonomyReader.getSize());
            assertTrue(fr.getNumValidDescendants() > 0);
            assertFalse(root.subResults.isEmpty());
          }

          //if (VERBOSE) {
          //System.out.println("TEST: facets=" + FacetTestUtils.toSimpleString(results.get(0)));
          //}
        } finally {
          mgr.release(pair);
        }
      }
    } finally {
      indexer.join();
      reopener.join();
    }

    if (VERBOSE) {
      System.out.println("TEST: now stop");
    }

    IOUtils.close(mgr, tw, w, taxoDir, dir);
  }

  public void testReplaceTaxonomy() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir);

    Directory taxoDir2 = newDirectory();
    DirectoryTaxonomyWriter tw2 = new DirectoryTaxonomyWriter(taxoDir2);
    tw2.close();

    SearcherTaxonomyManager mgr = new SearcherTaxonomyManager(w, true, null, tw);
    w.addDocument(new Document());
    tw.replaceTaxonomy(taxoDir2);
    taxoDir2.close();

    try {
      mgr.maybeRefresh();
      fail("should have hit exception");
    } catch (IllegalStateException ise) {
      // expected
    }

    IOUtils.close(mgr, tw, w, taxoDir, dir);
  }
}
