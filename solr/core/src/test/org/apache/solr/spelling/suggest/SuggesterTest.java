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
package org.apache.solr.spelling.suggest;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SuggesterTest extends SolrTestCaseJ4 {
  /**
   * Expected URI at which the given suggester will live.
   */
  protected String requestUri = "/suggest";

  // TODO: fix this test to not require FSDirectory
  static String savedFactory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    initCore("solrconfig-spellchecker.xml","schema-spellchecker.xml");
  }
  
  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  public static void addDocs() {
    assertU(adoc("id", "1",
                 "text", "acceptable accidentally accommodate acquire"
               ));
    assertU(adoc("id", "2",
                 "text", "believe bellwether accommodate acquire"
               ));
    assertU(adoc("id", "3",
                "text", "cemetery changeable conscientious consensus acquire bellwether"
               ));
  }
  
  @Test
  public void testSuggestions() throws Exception {
    addDocs();
    assertU(commit()); // configured to do a rebuild on commit

    assertQ(req("qt", requestUri, "q", "ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[1][.='acquire']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[2][.='accommodate']"
    );
  }
  
  @Test
  public void testReload() throws Exception {
    addDocs();
    assertU(commit());

    h.reload();
    // wait until the new searcher is registered
    waitForWarming();
    
    assertQ(req("qt", requestUri, "q", "ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[1][.='acquire']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[2][.='accommodate']"
        );
  }
  
  @Test
  public void testRebuild() throws Exception {
    addDocs();
    assertU(commit());
    assertQ(req("qt", requestUri, "q", "ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']");
    assertU(adoc("id", "4",
        "text", "actually"
       ));
    assertU(commit());
    assertQ(req("qt", requestUri, "q", "ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']");
  }
  
  // SOLR-2726
  @SuppressWarnings({"unchecked"})
  public void testAnalyzer() throws Exception {
    Suggester suggester = new Suggester();
    @SuppressWarnings({"rawtypes"})
    NamedList params = new NamedList();
    params.add("field", "test_field");
    params.add("lookupImpl", "org.apache.solr.spelling.suggest.tst.TSTLookupFactory");
    suggester.init(params, h.getCore());
    assertTrue(suggester.getQueryAnalyzer() != null);
  }
}
