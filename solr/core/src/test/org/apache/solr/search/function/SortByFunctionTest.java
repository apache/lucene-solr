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
package org.apache.solr.search.function;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;


/**
 *
 * @see TestSortByMinMaxFunction
 **/
public class SortByFunctionTest extends SolrTestCaseJ4 {

  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
    assertU(commit());
  }

  public void test() throws Exception {
    assertU(adoc("id", "1", "x_td1", "0", "y_td1", "2", "w_td1", "25", "z_td1", "5", "f_t", "ipod"));
    assertU(adoc("id", "2", "x_td1", "2", "y_td1", "2", "w_td1", "15", "z_td1", "5", "f_t", "ipod ipod ipod ipod ipod"));
    assertU(adoc("id", "3", "x_td1", "3", "y_td1", "2", "w_td1", "55", "z_td1", "5", "f_t", "ipod ipod ipod ipod ipod ipod ipod ipod ipod"));
    assertU(adoc("id", "4", "x_td1", "4", "y_td1", "2", "w_td1", "45", "z_td1", "5", "f_t", "ipod ipod ipod ipod ipod ipod ipod"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "*:*"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/str[@name='id'][.='1']",
            "//result/doc[2]/str[@name='id'][.='2']",
            "//result/doc[3]/str[@name='id'][.='3']",
            "//result/doc[4]/str[@name='id'][.='4']"
    );
    assertQ(req("fl", "*,score", "q", "*:*", "sort", "score desc"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/str[@name='id'][.='1']",
            "//result/doc[2]/str[@name='id'][.='2']",
            "//result/doc[3]/str[@name='id'][.='3']",
            "//result/doc[4]/str[@name='id'][.='4']"
    );
    assertQ(req("fl", "id,score", "q", "f_t:ipod", "sort", "score desc"),
            "//*[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='1']",
            "//result/doc[2]/str[@name='id'][.='2']",
            "//result/doc[3]/str[@name='id'][.='3']",
            "//result/doc[4]/str[@name='id'][.='4']"
    );


    assertQ(req("fl", "*,score", "q", "*:*", "sort", "sum(x_td1, y_td1) desc"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/str[@name='id'][.='4']",
            "//result/doc[2]/str[@name='id'][.='3']",
            "//result/doc[3]/str[@name='id'][.='2']",
            "//result/doc[4]/str[@name='id'][.='1']"
    );
    assertQ(req("fl", "*,score", "q", "*:*", "sort", "sum(x_td1, y_td1) asc"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/str[@name='id'][.='1']",
            "//result/doc[2]/str[@name='id'][.='2']",
            "//result/doc[3]/str[@name='id'][.='3']",
            "//result/doc[4]/str[@name='id'][.='4']"
    );
    //the function is equal, w_td1 separates
    assertQ(req("q", "*:*", "fl", "id", "sort", "sum(z_td1, y_td1) asc, w_td1 asc"),
            "//*[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='2']",
            "//result/doc[2]/str[@name='id'][.='1']",
            "//result/doc[3]/str[@name='id'][.='4']",
            "//result/doc[4]/str[@name='id'][.='3']"
    );
  }
  
  public void testSortJoinDocFreq() throws Exception
  {
    assertU(adoc("id", "4", "id_s1", "D", "links_mfacet", "A", "links_mfacet", "B", "links_mfacet", "C" ) );
    assertU(adoc("id", "3", "id_s1", "C", "links_mfacet", "A", "links_mfacet", "B" ) );
    assertU(commit()); // Make sure it uses two readers
    assertU(adoc("id", "2", "id_s1", "B", "links_mfacet", "A" ) );
    assertU(adoc("id", "1", "id_s1", "A"  ) );
    assertU(commit());

    assertQ(req("q", "links_mfacet:B", "fl", "id", "sort", "id asc"),
            "//*[@numFound='2']",
            "//result/doc[1]/str[@name='id'][.='3']",
            "//result/doc[2]/str[@name='id'][.='4']"
    );
    
    assertQ(req("q", "*:*", "fl", "id", "sort", "joindf(id_s1, links_mfacet) desc"),
            "//*[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='1']",
            "//result/doc[2]/str[@name='id'][.='2']",
            "//result/doc[3]/str[@name='id'][.='3']",
            "//result/doc[4]/str[@name='id'][.='4']"
    );

    assertQ(req("q", "*:*", "fl", "id", "sort", "joindf(id_s1, links_mfacet) asc"),
            "//*[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='4']",
            "//result/doc[2]/str[@name='id'][.='3']",
            "//result/doc[3]/str[@name='id'][.='2']",
            "//result/doc[4]/str[@name='id'][.='1']"
    );
  }
  
  /**
   * The sort clauses to test in <code>testFieldSortSpecifiedAsFunction</code>.
   *
   * @see #testFieldSortSpecifiedAsFunction
   */
  protected String[] getFieldFunctionClausesToTest() {
    return new String[] { "primary_tl1", "field(primary_tl1)" };
  }
  

  /**
   * Sort by function normally compares the double value, but if a function is specified that identifies
   * a single field, we should use the underlying field's SortField to save of a lot of type converstion 
   * (and RAM), and keep the sort precision as high as possible
   *
   * @see #getFieldFunctionClausesToTest
   */
  public void testFieldSortSpecifiedAsFunction() throws Exception {
    final long A = Long.MIN_VALUE;
    final long B = A + 1L;
    final long C = B + 1L;
    
    final long Z = Long.MAX_VALUE;
    final long Y = Z - 1L;
    final long X = Y - 1L;
    
    // test is predicated on the idea that if long -> double converstion is happening under the hood
    // then we lose precision in sorting; so lets sanity check that our JVM isn't doing something wacky
    // in converstion that violates the principle of the test
    
    assertEquals("WTF? small longs cast to double aren't equivalent?",
                 (double)A, (double)B, 0.0D);
    assertEquals("WTF? small longs cast to double aren't equivalent?",
                 (double)A, (double)C, 0.0D);
    
    assertEquals("WTF? big longs cast to double aren't equivalent?",
                 (double)Z, (double)Y, 0.0D);
    assertEquals("WTF? big longs cast to double aren't equivalent?",
                 (double)Z, (double)X, 0.0D);
    
    int docId = 0;
    for (int i = 0; i < 3; i++) {
      assertU(adoc(sdoc("id", ++docId, "primary_tl1", X, "secondary_tl1", i,
                        "multi_l_dv", X, "multi_l_dv", A)));
      assertU(adoc(sdoc("id", ++docId, "primary_tl1", Y, "secondary_tl1", i,
                        "multi_l_dv", Y, "multi_l_dv", B)));
      assertU(adoc(sdoc("id", ++docId, "primary_tl1", Z, "secondary_tl1", i,
                        "multi_l_dv", Z, "multi_l_dv", C)));
    }
    assertU(commit());

    // all of these sorts should result in the exact same order
    // min/max of a field is tested in TestSortByMinMaxFunction
    for (String primarySort : getFieldFunctionClausesToTest()) {

      assertQ(req("q", "*:*",
                  "sort", primarySort + " asc, secondary_tl1 asc")
              , "//*[@numFound='9']"
              //
              , "//result/doc[1]/long[@name='primary_tl1'][.='"+X+"']"
              , "//result/doc[1]/long[@name='secondary_tl1'][.='0']"
              , "//result/doc[2]/long[@name='primary_tl1'][.='"+X+"']"
              , "//result/doc[2]/long[@name='secondary_tl1'][.='1']"
              , "//result/doc[3]/long[@name='primary_tl1'][.='"+X+"']"
              , "//result/doc[3]/long[@name='secondary_tl1'][.='2']"
              //
              , "//result/doc[4]/long[@name='primary_tl1'][.='"+Y+"']"
              , "//result/doc[4]/long[@name='secondary_tl1'][.='0']"
              , "//result/doc[5]/long[@name='primary_tl1'][.='"+Y+"']"
              , "//result/doc[5]/long[@name='secondary_tl1'][.='1']"
              , "//result/doc[6]/long[@name='primary_tl1'][.='"+Y+"']"
              , "//result/doc[6]/long[@name='secondary_tl1'][.='2']"
              //
              , "//result/doc[7]/long[@name='primary_tl1'][.='"+Z+"']"
              , "//result/doc[7]/long[@name='secondary_tl1'][.='0']"
              , "//result/doc[8]/long[@name='primary_tl1'][.='"+Z+"']"
              , "//result/doc[8]/long[@name='secondary_tl1'][.='1']"
              , "//result/doc[9]/long[@name='primary_tl1'][.='"+Z+"']"
              , "//result/doc[9]/long[@name='secondary_tl1'][.='2']"
              );
    }
  }
  
}
