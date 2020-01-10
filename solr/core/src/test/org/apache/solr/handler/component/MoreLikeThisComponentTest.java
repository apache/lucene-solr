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
package org.apache.solr.handler.component;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for MoreLikeThisComponent
 *
 *
 * @see MoreLikeThisComponent
 */
@Slow
public class MoreLikeThisComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void moreLikeThisBeforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    assertU(adoc("id","42","name","Tom Cruise","subword","Top Gun","subword","Risky Business","subword","The Color of Money","subword","Minority Report","subword", "Days of Thunder","subword", "Eyes Wide Shut","subword", "Far and Away", "foo_ti","10"));
    assertU(adoc("id","43","name","Tom Hanks","subword","The Green Mile","subword","Forest Gump","subword","Philadelphia Story","subword","Big","subword","Cast Away", "foo_ti","10"));
    assertU(adoc("id","44","name","Harrison Ford","subword","Star Wars","subword","Indiana Jones","subword","Patriot Games","subword","Regarding Henry"));
    assertU(adoc("id","45","name","George Harrison","subword","Yellow Submarine","subword","Help","subword","Magical Mystery Tour","subword","Sgt. Peppers Lonley Hearts Club Band"));
    assertU(adoc("id","46","name","Nicole Kidman","subword","Batman","subword","Days of Thunder","subword","Eyes Wide Shut","subword","Far and Away"));
    assertU(commit());
  }
  
  private void initCommonMoreLikeThisParams(ModifiableSolrParams params) {
    params.set(MoreLikeThisParams.MLT, "true");
    params.set(MoreLikeThisParams.SIMILARITY_FIELDS, "name,subword");
    params.set(MoreLikeThisParams.MIN_TERM_FREQ,"1");
    params.set(MoreLikeThisParams.MIN_DOC_FREQ,"1");
    params.set("indent","true");
  }

  @Test
  public void testMLT_baseParams_shouldReturnSimilarDocuments()
  {
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    
    params.set(CommonParams.Q, "id:42");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params);
    assertQ("morelikethis - tom cruise",mltreq
        ,"//result/doc[1]/str[@name='id'][.='46']"
        ,"//result/doc[2]/str[@name='id'][.='43']");

    params.set(CommonParams.Q, "id:44");
    mltreq.close(); mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ("morelike this - harrison ford",mltreq
        ,"//result/doc[1]/str[@name='id'][.='45']");
    mltreq.close();
  }

  @Test
  public void testMLT_baseParamsInterestingTermsDetails_shouldReturnSimilarDocumentsAndInterestingTermsDetails()
  {
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "details");
    
    params.set(CommonParams.Q, "id:42");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params);
    assertQ("morelikethis - tom cruise",mltreq
        ,"//result/doc[1]/str[@name='id'][.='46']"
        ,"//result/doc[2]/str[@name='id'][.='43']",
        "//lst[@name='interestingTerms']/lst[1][count(*)>0]",
        "//lst[@name='interestingTerms']/lst[1]/float[.=1.0]");
    mltreq.close();
  }

  @Test
  public void testMLT_baseParamsInterestingTermsList_shouldReturnSimilarDocumentsAndInterestingTermsList()
  {
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "list");

    params.set(CommonParams.Q, "id:42");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params);
    assertQ("morelikethis - tom cruise",mltreq
        ,"//result/doc[1]/str[@name='id'][.='46']"
        ,"//result/doc[2]/str[@name='id'][.='43']",
        "//lst[@name='interestingTerms']/arr[@name='42'][count(*)>0]",
        "//lst[@name='interestingTerms']/arr[@name='42']/str[.='name:Cruise']");
    mltreq.close();
  }

  @Test
  public void testMLT_boostEnabled_shouldReturnSimilarDocumentsConsideringBoost()
  {
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.BOOST, "true");

    params.set(CommonParams.Q, "id:42");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params);
    assertQ("morelikethis - tom cruise",mltreq
        ,"//result/doc[1]/str[@name='id'][.='46']"
        ,"//result/doc[2]/str[@name='id'][.='43']");

    params.set(CommonParams.Q, "id:42");
    params.set(MoreLikeThisParams.QF,"name^5.0 subword^0.1");
    mltreq.close(); mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ("morelikethis with weights",mltreq
        ,"//result/doc[1]/str[@name='id'][.='43']"
        ,"//result/doc[2]/str[@name='id'][.='46']");

    mltreq.close();
  }

  @Test
  public void testMLT_boostEnabledInterestingTermsDetails_shouldReturnSimilarDocumentsConsideringBoostAndInterestingTermsDetails()
  {
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.BOOST, "true");
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "details");
    
    params.set(CommonParams.Q, "id:42");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params);
    assertQ("morelikethis - tom cruise",mltreq
        ,"//result/doc[1]/str[@name='id'][.='46']"
        ,"//result/doc[2]/str[@name='id'][.='43']",
        "//lst[@name='interestingTerms']/lst[1][count(*)>0]",
        "//lst[@name='interestingTerms']/lst[1]/float[.>1.0]");
    
    params.set(MoreLikeThisParams.QF,"name^5.0 subword^0.1");
    mltreq.close(); mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ("morelikethis with weights",mltreq
        ,"//result/doc[1]/str[@name='id'][.='43']"
        ,"//result/doc[2]/str[@name='id'][.='46']",
        "//lst[@name='interestingTerms']/lst[1][count(*)>0]",
        "//lst[@name='interestingTerms']/lst[1]/float[.>5.0]");

    mltreq.close();
  }

  @Test
  public void testMLT_boostEnabledInterestingTermsList_shouldReturnSimilarDocumentsConsideringBoostAndInterestingTermsList()
  {
    SolrCore core = h.getCore();
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.BOOST, "true");
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "list");

    params.set(CommonParams.Q, "id:42");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest( core, params);
    assertQ("morelikethis - tom cruise",mltreq
        ,"//result/doc[1]/str[@name='id'][.='46']"
        ,"//result/doc[2]/str[@name='id'][.='43']",
        "//lst[@name='interestingTerms']/arr[@name='42'][count(*)>0]",
        "//lst[@name='interestingTerms']/arr[@name='42']/str[.='name:Cruise']");

    params.set(MoreLikeThisParams.QF,"name^5.0 subword^0.1");
    mltreq.close(); mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ("morelikethis with weights",mltreq
        ,"//result/doc[1]/str[@name='id'][.='43']"
        ,"//result/doc[2]/str[@name='id'][.='46']",
        "//lst[@name='interestingTerms']/arr[@name='42'][count(*)>0]",
        "//lst[@name='interestingTerms']/arr[@name='42']/str[.='name:Cruise']");

    mltreq.close();
  }

  @Test
  public void testMLT_debugEnabled_shouldReturnSimilarDocumentsWithDebug()
  {
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.BOOST, "true");

    params.set(CommonParams.Q, "id:44");
    params.set(CommonParams.DEBUG_QUERY, "true");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ("morelike this - harrison ford",mltreq
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='rawMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='boostedMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='realMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/lst[@name='explain']/str[@name='45']"
    );
    
    params.remove(CommonParams.DEBUG_QUERY);
    params.set(CommonParams.Q, "{!field f=id}44");
    mltreq.close(); mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ(mltreq
        ,"//result/doc[1]/str[@name='id'][.='45']");
    mltreq.close();
  }

  @Test
  public void testMLT_debugEnabledInterestingTermsDetails_shouldReturnSimilarDocumentsWithDebugAndInterestingTermsDetails()
  {
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.BOOST, "true");
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "details");

    params.set(CommonParams.Q, "id:44");
    params.set(CommonParams.DEBUG_QUERY, "true");
    SolrQueryRequest mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ("morelike this - harrison ford",mltreq
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='rawMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='boostedMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='realMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/lst[@name='explain']/str[@name='45']",
        "//lst[@name='interestingTerms']/lst[1][count(*)>0]",
        "//lst[@name='interestingTerms']/lst[1]/float[.>1.0]");

    params.remove(CommonParams.DEBUG_QUERY);
    params.set(CommonParams.Q, "{!field f=id}44");
    mltreq.close(); mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ(mltreq
        ,"//result/doc[1]/str[@name='id'][.='45']",
        "//lst[@name='interestingTerms']/lst[1][count(*)>0]",
        "//lst[@name='interestingTerms']/lst[1]/float[.>1.0]");
    mltreq.close();
  }

  @Test
  public void testMLT_debugEnabledInterestingTermsList_shouldReturnSimilarDocumentsWithDebugAndInterestingTermsList()
  {
    ModifiableSolrParams params = new ModifiableSolrParams();

    initCommonMoreLikeThisParams(params);
    params.set(MoreLikeThisParams.BOOST, "true");
    params.set(MoreLikeThisParams.INTERESTING_TERMS, "list");

    params.set(CommonParams.Q, "id:44");
    params.set(CommonParams.DEBUG_QUERY, "true");
    
    SolrQueryRequest mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ("morelike this - harrison ford",mltreq
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='rawMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='boostedMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/str[@name='realMLTQuery']"
        ,"//lst[@name='debug']/lst[@name='moreLikeThis']/lst[@name='44']/lst[@name='explain']/str[@name='45']",
        "//lst[@name='interestingTerms']/arr[@name='44'][count(*)>0]",
        "//lst[@name='interestingTerms']/arr[@name='44']/str[.='name:Harrison']");

    params.remove(CommonParams.DEBUG_QUERY);
    params.set(CommonParams.Q, "{!field f=id}44");
    mltreq.close(); mltreq = new LocalSolrQueryRequest(h.getCore(), params);
    assertQ(mltreq
        ,"//result/doc[1]/str[@name='id'][.='45']",
        "//lst[@name='interestingTerms']/arr[@name='44'][count(*)>0]",
        "//lst[@name='interestingTerms']/arr[@name='44']/str[.='name:Harrison']");
    mltreq.close();
  }
}
