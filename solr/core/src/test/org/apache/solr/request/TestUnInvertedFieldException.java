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
package org.apache.solr.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.facet.UnInvertedField;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Test;

public class TestUnInvertedFieldException extends SolrTestCaseJ4 {

  private int numTerms;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    initCore("solrconfig.xml","schema11.xml");
    numTerms = TestUtil.nextInt(random(), 10, 50);
    createIndex(numTerms);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    deleteCore();
    super.tearDown();
  }

  String t(int tnum) {
    return String.format(Locale.ROOT, "%08d", tnum);
  }
  
  void createIndex(int nTerms) {
    assertU(delQ("*:*"));
    for (int i=0; i<nTerms; i++) {
      assertU(adoc("id", Integer.toString(i), proto.field(), t(i) ));
    }
    assertU(commit()); 
  }

  Term proto = new Term("field_s","");

  @Test
  public void testConcurrentInit() throws Exception {
    try (SolrQueryRequest req = req("*:*")) {
      final SolrIndexSearcher searcher = req.getSearcher();

      List<Callable<UnInvertedField>> initCallables = new ArrayList<>();
      for (int i = 0; i < TestUtil.nextInt(random(), 10, 30); i++) {
        initCallables.add(() -> UnInvertedField.getUnInvertedField(proto.field(), searcher));
      }

      final ExecutorService pool = getTestExecutor();

      TestInjection.uifOutOfMemoryError = true;
      if (LuceneTestCase.assertsAreEnabled) { // if they aren't, we check that injection is disabled in live
        List<Future<UnInvertedField>> futures = initCallables.stream().map((c) -> pool.submit(c)).collect(Collectors.toList());
        for (Future<UnInvertedField> uifuture : futures) {
          try {
            final UnInvertedField uif = uifuture.get();
          } catch (ExecutionException injection) {
            SolrException solrException = (SolrException) injection.getCause();
            assertEquals(ErrorCode.SERVER_ERROR.code, solrException.code());
            assertSame(solrException.getCause().getClass(), OutOfMemoryError.class);
          }
          assertNull(UnInvertedField.checkUnInvertedField(proto.field(), searcher));
        }
        TestInjection.uifOutOfMemoryError = false;
      }
      UnInvertedField prev = null;
      List<Future<UnInvertedField>> futures = initCallables.stream().map((c) -> pool.submit(c)).collect(Collectors.toList());
      for (Future<UnInvertedField> uifuture : futures) {
        final UnInvertedField uif = uifuture.get();
        assertNotNull(uif);
        assertSame(uif, UnInvertedField.checkUnInvertedField(proto.field(), searcher));
        if (prev != null) {
          assertSame(prev, uif);
        }
        assertEquals(numTerms, uif.numTerms());
        prev = uif;
      }
    }
  }
}

