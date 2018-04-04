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
package org.apache.solr.analysis;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/**
 * Tests that charfilters are being applied properly
 * (e.g. once and only once) with mockcharfilter.
 */
public class TestCharFilters extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-charfilters.xml");
    // add some docs
    assertU(adoc("id", "1", "content", "aab"));
    assertU(adoc("id", "2", "content", "aabaa"));
    assertU(adoc("id", "3", "content2", "ab"));
    assertU(adoc("id", "4", "content2", "aba"));
    assertU(commit());
  }
  
  /**
   * Test query analysis: at querytime MockCharFilter will
   * double the 'a', so ab -&gt; aab, and aba -&gt; aabaa
   * 
   * We run the test twice to make sure reuse is working
   */
  public void testQueryAnalysis() {
    assertQ("Query analysis: ",
       req("fl", "id", "q", "content:ab", "sort", "id asc"),
                "//*[@numFound='1']",
                "//result/doc[1]/str[@name='id'][.=1]"
    );
    assertQ("Query analysis: ",
        req("fl", "id", "q", "content:aba", "sort", "id asc"),
                 "//*[@numFound='1']",
                 "//result/doc[1]/str[@name='id'][.=2]"
    );
  }
  
  /**
   * Test index analysis: at indextime MockCharFilter will
   * double the 'a', so ab -&gt; aab, and aba -&gt; aabaa
   * 
   * We run the test twice to make sure reuse is working
   */
  public void testIndexAnalysis() {
    assertQ("Index analysis: ",
       req("fl", "id", "q", "content2:aab", "sort", "id asc"),
                "//*[@numFound='1']",
                "//result/doc[1]/str[@name='id'][.=3]"
    );
    assertQ("Index analysis: ",
        req("fl", "id", "q", "content2:aabaa", "sort", "id asc"),
                 "//*[@numFound='1']",
                 "//result/doc[1]/str[@name='id'][.=4]"
    );
  }
}
