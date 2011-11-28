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

package org.apache.solr.analysis;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/**
 * Tests {@link CollationKeyFilterFactory} with RangeQueries
 */
public class TestCollationKeyRangeQueries extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-collatefilter.xml");
    // add some docs
    assertU(adoc("id", "1", "text", "\u0633\u0627\u0628"));
    assertU(adoc("id", "2", "text", "I WİLL USE TURKİSH CASING"));
    assertU(adoc("id", "3", "text", "ı will use turkish casıng"));
    assertU(adoc("id", "4", "text", "Töne"));
    assertU(adoc("id", "5", "text", "I W\u0049\u0307LL USE TURKİSH CASING"));
    assertU(adoc("id", "6", "text", "Ｔｅｓｔｉｎｇ"));
    assertU(adoc("id", "7", "text", "Tone"));
    assertU(adoc("id", "8", "text", "Testing"));
    assertU(adoc("id", "9", "text", "testing"));
    assertU(adoc("id", "10", "text", "toene"));
    assertU(adoc("id", "11", "text", "Tzne"));
    assertU(adoc("id", "12", "text", "\u0698\u0698"));
    assertU(commit());
  }
  
  /** 
   * Test termquery with german DIN 5007-1 primary strength.
   * In this case, ö is equivalent to o (but not oe) 
   */
  public void testBasicTermQuery() {
    assertQ("Collated TQ: ",
       req("fl", "id", "q", "sort_de:tone", "sort", "id asc" ),
              "//*[@numFound='2']",
              "//result/doc[1]/int[@name='id'][.=4]",
              "//result/doc[2]/int[@name='id'][.=7]"
    );
  }
  
  /** 
   * Test rangequery again with the DIN 5007-1 collator.
   * We do a range query of tone .. tp, in binary order this
   * would retrieve nothing due to case and accent differences.
   */
  public void testBasicRangeQuery() {
    assertQ("Collated RangeQ: ",
        req("fl", "id", "q", "sort_de:[tone TO tp]", "sort", "id asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/int[@name='id'][.=4]",
               "//result/doc[2]/int[@name='id'][.=7]"
     );
  }

  /** 
   * Test rangequery again with an Arabic collator.
   * Binary order would normally order U+0633 in this range.
   */
  public void testNegativeRangeQuery() {
    assertQ("Collated RangeQ: ",
        req("fl", "id", "q", "sort_ar:[\u062F TO \u0698]", "sort", "id asc" ),
               "//*[@numFound='0']"
     );
  }
}
