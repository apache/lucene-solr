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

package org.apache.solr.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.request.SolrQueryRequest;

import java.util.Random;

/**
 * @version $Id$
 */
public class TestSearchPerf extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema11.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  public void setUp() throws Exception {
    super.setUp();
  }
  public void tearDown() throws Exception {
    close();
    super.tearDown();
  }

  String t(int tnum) {
    return String.format("%08d", tnum);
  }

  Random r = new Random(0);
  int nDocs;
  void createIndex(int nDocs) {
    this.nDocs = nDocs;
    assertU(delQ("*:*"));
    for (int i=0; i<nDocs; i++) {
      assertU(adoc("id", Float.toString(i)
              ,"foo1_s",t(0)
              ,"foo2_s",t(r.nextInt(2))
              ,"foo4_s",t(r.nextInt(3))
      ));
    }
    // assertU(optimize()); // squeeze out any possible deleted docs
    assertU(commit());
  }

  SolrQueryRequest req; // used to get a searcher
  void close() {
    if (req!=null) req.close();
    req = null;
  }

  int doSetGen(int iter, Query q) throws Exception {
    close();
    req = lrf.makeRequest("q","*:*");

    SolrIndexSearcher searcher = req.getSearcher();

    long start = System.currentTimeMillis();

    int ret = 0;
    for (int i=0; i<iter; i++) {
      DocSet set = searcher.getDocSetNC(q, null);
      ret += set.size();
    }

    long end = System.currentTimeMillis();
    System.out.println("ret="+ret+ " time="+(end-start)+" throughput="+iter*1000/(end-start));

    return ret;
  }

  // prevent complaints by junit
  public void testEmpty() {
  }

  public void XtestSetGenerationPerformance() throws Exception {
    createIndex(49999);
    doSetGen(10000, new TermQuery(new Term("foo1_s",t(0))) );

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("foo2_s",t(0))), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("foo2_s",t(1))), BooleanClause.Occur.SHOULD);
    doSetGen(5000, bq); 
  }

}