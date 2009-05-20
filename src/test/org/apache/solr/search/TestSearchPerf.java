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
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.SolrInputDocument;

import java.util.Random;
import java.io.IOException;

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
 //             ,"foo1_s",t(0)
 //             ,"foo2_s",t(r.nextInt(2))
 //             ,"foo4_s",t(r.nextInt(3))
              ,"foomany_s",t(r.nextInt(nDocs*10))
      ));
    }
    // assertU(optimize()); // squeeze out any possible deleted docs
    assertU(commit());
  }


  void createIndex2(int nDocs) throws IOException {
    SolrQueryRequest req = lrf.makeRequest();
    SolrQueryResponse rsp = new SolrQueryResponse();
    UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessingChain(null);
    UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp);

    for (int i=0; i<nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id",Float.toString(i));
      doc.addField("foomany_s",t(r.nextInt(nDocs*10)));
      AddUpdateCommand cmd = new AddUpdateCommand();
      cmd.solrDoc = doc;
      processor.processAdd(cmd);
    }
    processor.finish();
    req.close();

    assertU(commit());

    req = lrf.makeRequest();
    assertEquals(nDocs, req.getSearcher().maxDoc());
    req.close();
  }


  int doSetGen(int iter, Query q) throws Exception {
    SolrQueryRequest req = lrf.makeRequest();

    SolrIndexSearcher searcher = req.getSearcher();

    long start = System.currentTimeMillis();

    int ret = 0;
    for (int i=0; i<iter; i++) {
      DocSet set = searcher.getDocSetNC(q, null);
      ret += set.size();
    }

    long end = System.currentTimeMillis();
    System.out.println("ret="+ret+ " time="+(end-start)+" throughput="+iter*1000/(end-start+1));

    req.close();
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

  /** test range query performance */
  public void XtestRangePerformance() throws Exception {
    int indexSize=199999;
    float fractionCovered=1.0f;

    String l=t(0);
    String u=t((int)(indexSize*10*fractionCovered));   

    SolrQueryRequest req = lrf.makeRequest();
    QParser parser = QParser.getParser("foomany_s:[" + l + " TO " + u + "]", null, req);
    Query range = parser.parse();
                                     
    QParser parser2 = QParser.getParser("{!frange l="+l+" u="+u+"}foomany_s", null, req);
    Query frange = parser2.parse();
    req.close();

    createIndex2(indexSize);

    doSetGen(1, range);
    doSetGen(1, frange);   // load field cache

    doSetGen(100, range);
    doSetGen(10000, frange);
  }

}