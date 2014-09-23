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
import java.util.Random;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.uninverting.DocTermOrds;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestFaceting extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema11.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    close();
    super.tearDown();
  }

  String t(int tnum) {
    return String.format(Locale.ROOT, "%08d", tnum);
  }
  
  void createIndex(int nTerms) {
    assertU(delQ("*:*"));
    for (int i=0; i<nTerms; i++) {
      assertU(adoc("id", Float.toString(i), proto.field(), t(i) ));
    }
    assertU(optimize()); // squeeze out any possible deleted docs
  }

  Term proto = new Term("field_s","");
  SolrQueryRequest req; // used to get a searcher
  void close() {
    if (req!=null) req.close();
    req = null;
  }

  void doTermEnum(int size) throws Exception {
    //System.out.println("doTermEnum size=" + size);
    close();
    createIndex(size);
    req = lrf.makeRequest("q","*:*");

    SortedSetDocValues dv = DocValues.getSortedSet(req.getSearcher().getLeafReader(), proto.field());

    assertEquals(size, dv.getValueCount());

    TermsEnum te = dv.termsEnum();

    Random r = new Random(size);
    // test seeking by term string
    for (int i=0; i<size*2+10; i++) {
      int rnum = r.nextInt(size+2);
      String s = t(rnum);
      //System.out.println("s=" + s);
      final BytesRef br;
      if (te == null) {
        br = null;
      } else {
        TermsEnum.SeekStatus status = te.seekCeil(new BytesRef(s));
        if (status == TermsEnum.SeekStatus.END) {
          br = null;
        } else {
          br = te.term();
        }
      }
      assertEquals(br != null, rnum < size);
      if (rnum < size) {
        assertEquals(rnum, (int) te.ord());
        assertEquals(s, te.term().utf8ToString());
      }
    }

    // test seeking before term
    if (size>0) {
      assertEquals(size>0, te.seekCeil(new BytesRef("000")) != TermsEnum.SeekStatus.END);
      assertEquals(0, te.ord());
      assertEquals(t(0), te.term().utf8ToString());
    }

    if (size>0) {
      // test seeking by term number
      for (int i=0; i<size*2+10; i++) {
        int rnum = r.nextInt(size);
        String s = t(rnum);
        te.seekExact((long) rnum);
        BytesRef br = te.term();
        assertNotNull(br);
        assertEquals(rnum, (int) te.ord());
        assertEquals(s, te.term().utf8ToString());
      }
    }
  }

  @Test
  public void testTermEnum() throws Exception {
    doTermEnum(0);
    doTermEnum(1);
    final int DEFAULT_INDEX_INTERVAL = 1 << DocTermOrds.DEFAULT_INDEX_INTERVAL_BITS;
    doTermEnum(DEFAULT_INDEX_INTERVAL - 1);  // test boundaries around the block size
    doTermEnum(DEFAULT_INDEX_INTERVAL);
    doTermEnum(DEFAULT_INDEX_INTERVAL + 1);
    doTermEnum(DEFAULT_INDEX_INTERVAL * 2 + 2);    
    // doTermEnum(DEFAULT_INDEX_INTERVAL * 3 + 3);    
  }

  @Test
  public void testFacets() throws Exception {
    StringBuilder sb = new StringBuilder();

    // go over 4096 to test some of the buffer resizing
    for (int i=0; i<5000; i++) {
      sb.append(t(i));
      sb.append(' ');     
    }

    assertU(adoc("id", "1", "many_ws", sb.toString()));
    assertU(commit());

    assertQ("check many tokens",
            req("q", "id:1","indent","true"
                ,"facet", "true", "facet.method","fc"
                ,"facet.field", "many_ws"
                ,"facet.limit", "-1"
                )
            ,"*[count(//lst[@name='many_ws']/int)=5000]"
            ,"//lst[@name='many_ws']/int[@name='" + t(0) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(1) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(2) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(3) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(5) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4092) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4093) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4094) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4095) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4096) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4097) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4098) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4090) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4999) + "'][.='1']"
            );

    // test gaps that take more than one byte
    sb = new StringBuilder();
    sb.append(t(0)).append(' ');
    sb.append(t(150)).append(' ');
    sb.append(t(301)).append(' ');
    sb.append(t(453)).append(' ');
    sb.append(t(606)).append(' ');
    sb.append(t(1000)).append(' ');
    sb.append(t(2010)).append(' ');
    sb.append(t(3050)).append(' ');
    sb.append(t(4999)).append(' ');
    assertU(adoc("id", "2", "many_ws", sb.toString()));
    assertQ("check many tokens",
            req("q", "id:1","indent","true"
                ,"facet", "true", "facet.method","fc"
                ,"facet.field", "many_ws"
                ,"facet.limit", "-1"
                )
            ,"*[count(//lst[@name='many_ws']/int)=5000]"
            ,"//lst[@name='many_ws']/int[@name='" + t(0) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(150) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(301) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(453) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(606) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(1000) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(2010) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(3050) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(4999) + "'][.='1']"
              );
  }

  @Test
  public void testRegularBig() throws Exception {
    StringBuilder sb = new StringBuilder();

    // go over 4096 to test some of the buffer resizing
    int nTerms=7;
    for (int i=0; i<nTerms; i++) {
      sb.append(t(i));
      sb.append(' ');
    }

    int i1=1000000;

    // int iter=65536+10;
    int iter=1000;
    int commitInterval=iter/9;

    for (int i=0; i<iter; i++) {
      // assertU(adoc("id", t(i), "many_ws", many_ws + t(i1+i) + " " + t(i1*2+i)));
      assertU(adoc("id", t(i), "many_ws", t(i1+i) + " " + t(i1*2+i)));
      if (iter % commitInterval == 0) {
        assertU(commit());
      }
    }
    assertU(commit());

    for (int i=0; i<iter; i+=iter/10) {
    assertQ("check many tokens",
            req("q", "id:"+t(i),"indent","true"
                ,"facet", "true", "facet.method","fc"
                ,"facet.field", "many_ws"
                ,"facet.limit", "-1"
                ,"facet.mincount", "1"
                )
            ,"*[count(//lst[@name='many_ws']/int)=" + 2 + "]"
            ,"//lst[@name='many_ws']/int[@name='" + t(i1+i) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(i1*2+i) + "'][.='1']"
            );
    }

    int i=iter-1;
    assertQ("check many tokens",
            req("q", "id:"+t(i),"indent","true"
                ,"facet", "true", "facet.method","fc"
                ,"facet.field", "many_ws"
                ,"facet.limit", "-1"
                ,"facet.mincount", "1"

                )
            ,"*[count(//lst[@name='many_ws']/int)=" + 2 + "]"
            ,"//lst[@name='many_ws']/int[@name='" + t(i1+i) + "'][.='1']"
            ,"//lst[@name='many_ws']/int[@name='" + t(i1*2+i) + "'][.='1']"
            );
  }

  @Test
  public void testTrieFields() {
    // make sure that terms are correctly filtered even for trie fields that index several
    // terms for a single value
    List<String> fields = new ArrayList<>();
    fields.add("id");
    fields.add("7");
    final String[] suffixes = new String[] {"ti", "tis", "tf", "tfs", "tl", "tls", "td", "tds"};
    for (String suffix : suffixes) {
      fields.add("f_" + suffix);
      fields.add("42");
    }
    assertU(adoc(fields.toArray(new String[0])));
    assertU(commit());
    for (String suffix : suffixes) {
      for (String facetMethod : new String[] {FacetParams.FACET_METHOD_enum, FacetParams.FACET_METHOD_fc, FacetParams.FACET_METHOD_fcs}) {
        for (String facetSort : new String[] {FacetParams.FACET_SORT_COUNT, FacetParams.FACET_SORT_INDEX}) {
          for (String value : new String[] {"42", "43"}) { // match or not
            final String field = "f_" + suffix;
            assertQ("field=" + field + ",method=" + facetMethod + ",sort=" + facetSort,
                req("q", field + ":" + value, FacetParams.FACET, "true", FacetParams.FACET_FIELD, field, FacetParams.FACET_MINCOUNT, "0", FacetParams.FACET_SORT, facetSort, FacetParams.FACET_METHOD, facetMethod),
                "*[count(//lst[@name='" + field + "']/int)=1]"); // exactly 1 facet count
          }
        }
      }
    }
  }

  @Test
  public void testFacetSortWithMinCount() {
    assertU(adoc("id", "1.0", "f_td", "-420.126"));
    assertU(adoc("id", "2.0", "f_td", "-285.672"));
    assertU(adoc("id", "3.0", "f_td", "-1.218"));
    assertU(commit());

    assertQ(req("q", "*:*", FacetParams.FACET, "true", FacetParams.FACET_FIELD, "f_td", "f.f_td.facet.sort", FacetParams.FACET_SORT_INDEX),
        "*[count(//lst[@name='f_td']/int)=3]",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[1][@name='-420.126']",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[2][@name='-285.672']",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[3][@name='-1.218']");

    assertQ(req("q", "*:*", FacetParams.FACET, "true", FacetParams.FACET_FIELD, "f_td", "f.f_td.facet.sort", FacetParams.FACET_SORT_INDEX, FacetParams.FACET_MINCOUNT, "1", FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc),
        "*[count(//lst[@name='f_td']/int)=3]",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[1][@name='-420.126']",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[2][@name='-285.672']",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[3][@name='-1.218']");

    assertQ(req("q", "*:*", FacetParams.FACET, "true", FacetParams.FACET_FIELD, "f_td", "f.f_td.facet.sort", FacetParams.FACET_SORT_INDEX, FacetParams.FACET_MINCOUNT, "1", "indent","true"),
        "*[count(//lst[@name='f_td']/int)=3]",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[1][@name='-420.126']",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[2][@name='-285.672']",
        "//lst[@name='facet_fields']/lst[@name='f_td']/int[3][@name='-1.218']");
  }


  @Test
  public void testDateFacetsWithMultipleConfigurationForSameField() {
    clearIndex();
    final String f = "bday_dt";

    assertU(adoc("id", "1",  f, "1976-07-04T12:08:56.235Z"));
    assertU(adoc("id", "2",  f, "1976-07-05T00:00:00.000Z"));
    assertU(adoc("id", "3",  f, "1976-07-15T00:07:67.890Z"));
    assertU(commit());
    assertU(adoc("id", "4",  f, "1976-07-21T00:07:67.890Z"));
    assertU(adoc("id", "5",  f, "1976-07-13T12:12:25.255Z"));
    assertU(adoc("id", "6",  f, "1976-07-03T17:01:23.456Z"));
    assertU(adoc("id", "7",  f, "1976-07-12T12:12:25.255Z"));
    assertU(adoc("id", "8",  f, "1976-07-15T15:15:15.155Z"));
    assertU(adoc("id", "9",  f, "1907-07-12T13:13:23.235Z"));
    assertU(adoc("id", "10", f, "1976-07-03T11:02:45.678Z"));
    assertU(commit());
    assertU(adoc("id", "11", f, "1907-07-12T12:12:25.255Z"));
    assertU(adoc("id", "12", f, "2007-07-30T07:07:07.070Z"));
    assertU(adoc("id", "13", f, "1976-07-30T22:22:22.222Z"));
    assertU(adoc("id", "14", f, "1976-07-05T22:22:22.222Z"));
    assertU(commit());

    final String preFoo = "//lst[@name='facet_dates']/lst[@name='foo']";
    final String preBar = "//lst[@name='facet_dates']/lst[@name='bar']";

    assertQ("check counts for month of facet by day",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.date", "{!key=foo " +
                  "facet.date.start=1976-07-01T00:00:00.000Z " +
                  "facet.date.end=1976-07-01T00:00:00.000Z+1MONTH " +
                  "facet.date.gap=+1DAY " +
                  "facet.date.other=all " +
                "}" + f
                ,"facet.date", "{!key=bar " +
                  "facet.date.start=1976-07-01T00:00:00.000Z " +
                  "facet.date.end=1976-07-01T00:00:00.000Z+7DAY " +
                  "facet.date.gap=+1DAY " +
                "}" + f
              )
            // 31 days + pre+post+inner = 34
            ,"*[count("+preFoo+"/int)=34]"
            ,preFoo+"/int[@name='1976-07-01T00:00:00Z'][.='0'  ]"
            ,preFoo+"/int[@name='1976-07-02T00:00:00Z'][.='0'  ]"
            ,preFoo+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            // july4th = 2 because exists doc @ 00:00:00.000 on July5
            // (date faceting is inclusive)
            ,preFoo+"/int[@name='1976-07-04T00:00:00Z'][.='2'  ]"
            ,preFoo+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,preFoo+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,preFoo+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,preFoo+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-15T00:00:00Z'][.='2'  ]"
            ,preFoo+"/int[@name='1976-07-16T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-17T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-18T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-19T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-21T00:00:00Z'][.='1'  ]"
            ,preFoo+"/int[@name='1976-07-22T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-23T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-24T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-25T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-26T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-27T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-28T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-29T00:00:00Z'][.='0']"
            ,preFoo+"/int[@name='1976-07-30T00:00:00Z'][.='1'  ]"
            ,preFoo+"/int[@name='1976-07-31T00:00:00Z'][.='0']"

            ,preFoo+"/int[@name='before' ][.='2']"
            ,preFoo+"/int[@name='after'  ][.='1']"
            ,preFoo+"/int[@name='between'][.='11']"

            ,"*[count("+preBar+"/int)=7]"
            ,preBar+"/int[@name='1976-07-01T00:00:00Z'][.='0'  ]"
            ,preBar+"/int[@name='1976-07-02T00:00:00Z'][.='0'  ]"
            ,preBar+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            // july4th = 2 because exists doc @ 00:00:00.000 on July5
            // (date faceting is inclusive)
            ,preBar+"/int[@name='1976-07-04T00:00:00Z'][.='2'  ]"
            ,preBar+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,preBar+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,preBar+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
              );

      clearIndex();
      assertU(commit());
    }

    public void testSimpleFacetCountsWithMultipleConfigurationsForSameField() {
      clearIndex();
      String fname = "trait_ss";
      assertU(adoc("id", "42",
          fname, "Tool",
          fname, "Obnoxious",
          "name_s", "Zapp Brannigan"));

      assertU(adoc("id", "43" ,
                   "title_s", "Democratic Order of Planets"));
      assertU(commit());
  
      assertU(adoc("id", "44",
          fname, "Tool",
          "name_s", "The Zapper"));
  
      assertU(adoc("id", "45",
          fname, "Chauvinist",
          "title_s", "25 star General"));
  
      assertU(adoc("id", "46",
          fname, "Obnoxious",
          "subject_s", "Defeated the pacifists of the Gandhi nebula"));
  
      assertU(commit());
  
      assertU(adoc("id", "47",
          fname, "Pig",
          "text_t", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
      assertU(commit());
  
      assertQ("checking facets when one has missing=true&mincount=2 and the other has missing=false&mincount=0",
              req("q", "id:[42 TO 47]"
                  ,"facet", "true"
                  ,"facet.zeros", "false"
                  ,"fq", "id:[42 TO 45]"
                  ,"facet.field", "{!key=foo " +
                     "facet.mincount=0 "+
                     "facet.missing=false "+
                  "}"+fname
                  ,"facet.field", "{!key=bar " +
                     "facet.mincount=2 "+
                     "facet.missing=true "+
                  "}"+fname
                  )
              ,"*[count(//doc)=4]"
              ,"*[count(//lst[@name='foo']/int)=4]"
              ,"*[count(//lst[@name='bar']/int)=2]"
              ,"//lst[@name='foo']/int[@name='Tool'][.='2']"
              ,"//lst[@name='foo']/int[@name='Obnoxious'][.='1']"
              ,"//lst[@name='foo']/int[@name='Chauvinist'][.='1']"
              ,"//lst[@name='foo']/int[@name='Pig'][.='0']"
              ,"//lst[@name='foo']/int[@name='Tool'][.='2']"
              ,"//lst[@name='bar']/int[not(@name)][.='1']"
              );
  
      assertQ("checking facets when one has missing=true&mincount=2 and the other has missing=false&mincount=0",
              req("q", "id:[42 TO 47]"
                  ,"facet", "true"
                  ,"facet.zeros", "false"
                  ,"fq", "id:[42 TO 45]"
                  ,"facet.field", "{!key=foo " +
                      "facet.prefix=Too "+
                  "}"+fname
                  ,"facet.field", "{!key=bar " +
                      "facet.limit=2 "+
                      "facet.sort=false "+
                  "}"+fname
                  )
              ,"*[count(//doc)=4]"
              ,"*[count(//lst[@name='foo']/int)=1]"
              ,"*[count(//lst[@name='bar']/int)=2]"
              ,"//lst[@name='foo']/int[@name='Tool'][.='2']"
              ,"//lst[@name='bar']/int[@name='Chauvinist'][.='1']"
              ,"//lst[@name='bar']/int[@name='Obnoxious'][.='1']"
              );

      assertQ("localparams in one facet variant should not affect defaults in another: facet.sort vs facet.missing",
                  req("q", "id:[42 TO 47]"
                          ,"rows","0"
                          ,"facet", "true"
                          ,"fq", "id:[42 TO 45]"
                          ,"facet.field", "{!key=foo " +
                              "facet.sort=index" +
                          "}"+fname
                          ,"facet.field", "{!key=bar " +
                              "facet.missing=true" +
                          "}"+fname
                          )
                      // foo is in index order w/o missing
                      ,"*[count(//lst[@name='foo']/int)=4]"
                  ,"//lst[@name='foo']/int[1][@name='Chauvinist'][.='1']"
                  ,"//lst[@name='foo']/int[2][@name='Obnoxious'][.='1']"
                  ,"//lst[@name='foo']/int[3][@name='Pig'][.='0']"
                  ,"//lst[@name='foo']/int[4][@name='Tool'][.='2']"
                  // bar is in count order by default and includes missing
                  ,"*[count(//lst[@name='bar']/int)=5]"
                  ,"//lst[@name='bar']/int[1][@name='Tool'][.='2']"
                  // don't assume tie breaker for slots 3 & 4, behavior undefined?
                  ,"//lst[@name='bar']/int[4][@name='Pig'][.='0']"
                  ,"//lst[@name='bar']/int[5][not(@name)][.='1']"
                  );

      assertQ("localparams in one facet variant should not affect defaults in another: facet.mincount",
                  req("q", "id:[42 TO 47]"
                          ,"rows","0"
                          ,"facet", "true"
                          ,"fq", "id:[42 TO 45]"
                          ,"facet.field", "{!key=foo " +
                              "facet.mincount=2" +
                          "}"+fname
                          ,"facet.field", "{!key=bar}"+fname
                          )
                      // only Tool for foo
                      ,"*[count(//lst[@name='foo']/int)=1]"
                  ,"//lst[@name='foo']/int[1][@name='Tool'][.='2']"
                  // all for bar
                  ,"*[count(//lst[@name='bar']/int)=4]"
                  ,"//lst[@name='bar']/int[1][@name='Tool'][.='2']"
                  // don't assume tie breaker for slots 3 & 4, behavior undefined?
                  ,"//lst[@name='bar']/int[4][@name='Pig'][.='0']"
                  );

      assertQ("localparams in one facet variant should not affect defaults in another: facet.missing",
                  req("q", "id:[42 TO 47]"
                          ,"rows","0"
                          ,"facet", "true"
                          ,"fq", "id:[42 TO 45]"
                          ,"facet.field", "{!key=foo " +
                              "facet.missing=true" +
                          "}"+fname
                          ,"facet.field", "{!key=bar}"+fname
                          )
                      // foo includes missing
                      ,"*[count(//lst[@name='foo']/int)=5]"
                  ,"//lst[@name='foo']/int[1][@name='Tool'][.='2']"
                  // don't assume tie breaker for slots 3 & 4, behavior undefined?
                  ,"//lst[@name='foo']/int[4][@name='Pig'][.='0']"
                  ,"//lst[@name='foo']/int[5][not(@name)][.='1']"
                  // bar does not
                  ,"*[count(//lst[@name='bar']/int)=4]"
                  ,"//lst[@name='bar']/int[1][@name='Tool'][.='2']"
                  // don't assume tie breaker for slots 3 & 4, behavior undefined?
                  ,"//lst[@name='bar']/int[4][@name='Pig'][.='0']"
                  );

      assertQ("checking facets when local facet.prefix param used after regular/raw field faceting",
          req("q", "*:*"
              ,"facet", "true"
              ,"facet.field", fname
              ,"facet.field", "{!key=foo " +
              "facet.prefix=T "+
              "}"+fname
          )
          ,"*[count(//doc)=6]"
          ,"*[count(//lst[@name='" + fname + "']/int)=4]"
          ,"*[count(//lst[@name='foo']/int)=1]"
          ,"//lst[@name='foo']/int[@name='Tool'][.='2']"
      );

      assertQ("checking facets when local facet.prefix param used before regular/raw field faceting",
          req("q", "*:*"
              ,"facet", "true"
              ,"facet.field", "{!key=foo " +
              "facet.prefix=T "+
              "}"+fname
              ,"facet.field", fname
          )
          ,"*[count(//doc)=6]"
          ,"*[count(//lst[@name='" + fname + "']/int)=4]"
          ,"*[count(//lst[@name='foo']/int)=1]"
          ,"//lst[@name='foo']/int[@name='Tool'][.='2']"
      );

      final String foo_range_facet = "{!key=foo facet.range.gap=2}val_i";
      final String val_range_facet = "val_i";
      for (boolean toggle : new boolean[] { true, false }) {
          assertQ("local gap param mixed w/raw range faceting: " + toggle,
                      req("q", "*:*"
                              ,"facet", "true"
                              ,"rows", "0"
                              ,"facet.range.start", "0"
                              ,"facet.range.end", "10"
                              ,"facet.range.gap", "1"
                              ,"facet.range", (toggle ? foo_range_facet : val_range_facet)
                              ,"facet.range", (toggle ? val_range_facet : foo_range_facet)
                              )
                          ,"*[count(//lst[@name='val_i']/lst[@name='counts']/int)=10]"
                      ,"*[count(//lst[@name='foo']/lst[@name='counts']/int)=5]"
                      );
        }

      clearIndex();
      assertU(commit());
  }

  private void add50ocs() {
    // Gimme 50 docs with 10 facet fields each
    for (int idx = 0; idx < 50; ++idx) {
      String f0 = (idx % 2 == 0) ? "zero_2" : "zero_1";
      String f1 = (idx % 3 == 0) ? "one_3" : "one_1";
      String f2 = (idx % 4 == 0) ? "two_4" : "two_1";
      String f3 = (idx % 5 == 0) ? "three_5" : "three_1";
      String f4 = (idx % 6 == 0) ? "four_6" : "four_1";
      String f5 = (idx % 7 == 0) ? "five_7" : "five_1";
      String f6 = (idx % 8 == 0) ? "six_8" : "six_1";
      String f7 = (idx % 9 == 0) ? "seven_9" : "seven_1";
      String f8 = (idx % 10 == 0) ? "eight_10" : "eight_1";
      String f9 = (idx % 11 == 0) ? "nine_11" : "nine_1";
      assertU(adoc("id", Integer.toString(idx),
          "f0_ws", f0,
          "f1_ws", f1,
          "f2_ws", f2,
          "f3_ws", f3,
          "f4_ws", f4,
          "f5_ws", f5,
          "f6_ws", f6,
          "f7_ws", f7,
          "f8_ws", f8,
          "f9_ws", f9
      ));
    }

    assertU(commit());

  }

  @Test
  public void testThreadWait() throws Exception {

    add50ocs();
    // All I really care about here is the chance to fire off a bunch of threads to the UnIninvertedField.get method
    // to insure that we get into/out of the lock. Again, it's not entirely deterministic, but it might catch bad
    // stuff occasionally...
    assertQ("check threading, more threads than fields",
        req("q", "id:*", "indent", "true", "fl", "id", "rows", "1"
            , "facet", "true"
            , "facet.field", "f0_ws"
            , "facet.field", "f0_ws"
            , "facet.field", "f0_ws"
            , "facet.field", "f0_ws"
            , "facet.field", "f0_ws"
            , "facet.field", "f1_ws"
            , "facet.field", "f1_ws"
            , "facet.field", "f1_ws"
            , "facet.field", "f1_ws"
            , "facet.field", "f1_ws"
            , "facet.field", "f2_ws"
            , "facet.field", "f2_ws"
            , "facet.field", "f2_ws"
            , "facet.field", "f2_ws"
            , "facet.field", "f2_ws"
            , "facet.field", "f3_ws"
            , "facet.field", "f3_ws"
            , "facet.field", "f3_ws"
            , "facet.field", "f3_ws"
            , "facet.field", "f3_ws"
            , "facet.field", "f4_ws"
            , "facet.field", "f4_ws"
            , "facet.field", "f4_ws"
            , "facet.field", "f4_ws"
            , "facet.field", "f4_ws"
            , "facet.field", "f5_ws"
            , "facet.field", "f5_ws"
            , "facet.field", "f5_ws"
            , "facet.field", "f5_ws"
            , "facet.field", "f5_ws"
            , "facet.field", "f6_ws"
            , "facet.field", "f6_ws"
            , "facet.field", "f6_ws"
            , "facet.field", "f6_ws"
            , "facet.field", "f6_ws"
            , "facet.field", "f7_ws"
            , "facet.field", "f7_ws"
            , "facet.field", "f7_ws"
            , "facet.field", "f7_ws"
            , "facet.field", "f7_ws"
            , "facet.field", "f8_ws"
            , "facet.field", "f8_ws"
            , "facet.field", "f8_ws"
            , "facet.field", "f8_ws"
            , "facet.field", "f8_ws"
            , "facet.field", "f9_ws"
            , "facet.field", "f9_ws"
            , "facet.field", "f9_ws"
            , "facet.field", "f9_ws"
            , "facet.field", "f9_ws"
            , "facet.threads", "1000"
            , "facet.limit", "-1"
        )
        , "*[count(//lst[@name='facet_fields']/lst)=10]"
        , "*[count(//lst[@name='facet_fields']/lst/int)=20]"
    );

  }

  @Test
  public void testMultiThreadedFacets() throws Exception {
    add50ocs();
    assertQ("check no threading, threads == 0",
        req("q", "id:*", "indent", "true", "fl", "id", "rows", "1"
            , "facet", "true"
            , "facet.field", "f0_ws"
            , "facet.field", "f1_ws"
            , "facet.field", "f2_ws"
            , "facet.field", "f3_ws"
            , "facet.field", "f4_ws"
            , "facet.field", "f5_ws"
            , "facet.field", "f6_ws"
            , "facet.field", "f7_ws"
            , "facet.field", "f8_ws"
            , "facet.field", "f9_ws"
            , "facet.threads", "0"
            , "facet.limit", "-1"
        )
        , "*[count(//lst[@name='facet_fields']/lst)=10]"
        , "*[count(//lst[@name='facet_fields']/lst/int)=20]"
        , "//lst[@name='f0_ws']/int[@name='zero_1'][.='25']"
        , "//lst[@name='f0_ws']/int[@name='zero_2'][.='25']"
        , "//lst[@name='f1_ws']/int[@name='one_1'][.='33']"
        , "//lst[@name='f1_ws']/int[@name='one_3'][.='17']"
        , "//lst[@name='f2_ws']/int[@name='two_1'][.='37']"
        , "//lst[@name='f2_ws']/int[@name='two_4'][.='13']"
        , "//lst[@name='f3_ws']/int[@name='three_1'][.='40']"
        , "//lst[@name='f3_ws']/int[@name='three_5'][.='10']"
        , "//lst[@name='f4_ws']/int[@name='four_1'][.='41']"
        , "//lst[@name='f4_ws']/int[@name='four_6'][.='9']"
        , "//lst[@name='f5_ws']/int[@name='five_1'][.='42']"
        , "//lst[@name='f5_ws']/int[@name='five_7'][.='8']"
        , "//lst[@name='f6_ws']/int[@name='six_1'][.='43']"
        , "//lst[@name='f6_ws']/int[@name='six_8'][.='7']"
        , "//lst[@name='f7_ws']/int[@name='seven_1'][.='44']"
        , "//lst[@name='f7_ws']/int[@name='seven_9'][.='6']"
        , "//lst[@name='f8_ws']/int[@name='eight_1'][.='45']"
        , "//lst[@name='f8_ws']/int[@name='eight_10'][.='5']"
        , "//lst[@name='f9_ws']/int[@name='nine_1'][.='45']"
        , "//lst[@name='f9_ws']/int[@name='nine_11'][.='5']"

    );

    RefCounted<SolrIndexSearcher> currentSearcherRef = h.getCore().getSearcher();
    try {
      SolrIndexSearcher currentSearcher = currentSearcherRef.get();
      SortedSetDocValues ui0 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f0_ws");
      SortedSetDocValues ui1 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f1_ws");
      SortedSetDocValues ui2 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f2_ws");
      SortedSetDocValues ui3 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f3_ws");
      SortedSetDocValues ui4 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f4_ws");
      SortedSetDocValues ui5 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f5_ws");
      SortedSetDocValues ui6 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f6_ws");
      SortedSetDocValues ui7 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f7_ws");
      SortedSetDocValues ui8 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f8_ws");
      SortedSetDocValues ui9 = DocValues.getSortedSet(currentSearcher.getLeafReader(), "f9_ws");

      assertQ("check threading, more threads than fields",
          req("q", "id:*", "indent", "true", "fl", "id", "rows", "1"
              , "facet", "true"
              , "facet.field", "f0_ws"
              , "facet.field", "f1_ws"
              , "facet.field", "f2_ws"
              , "facet.field", "f3_ws"
              , "facet.field", "f4_ws"
              , "facet.field", "f5_ws"
              , "facet.field", "f6_ws"
              , "facet.field", "f7_ws"
              , "facet.field", "f8_ws"
              , "facet.field", "f9_ws"
              , "facet.threads", "1000"
              , "facet.limit", "-1"
          )
          , "*[count(//lst[@name='facet_fields']/lst)=10]"
          , "*[count(//lst[@name='facet_fields']/lst/int)=20]"
          , "//lst[@name='f0_ws']/int[@name='zero_1'][.='25']"
          , "//lst[@name='f0_ws']/int[@name='zero_2'][.='25']"
          , "//lst[@name='f1_ws']/int[@name='one_1'][.='33']"
          , "//lst[@name='f1_ws']/int[@name='one_3'][.='17']"
          , "//lst[@name='f2_ws']/int[@name='two_1'][.='37']"
          , "//lst[@name='f2_ws']/int[@name='two_4'][.='13']"
          , "//lst[@name='f3_ws']/int[@name='three_1'][.='40']"
          , "//lst[@name='f3_ws']/int[@name='three_5'][.='10']"
          , "//lst[@name='f4_ws']/int[@name='four_1'][.='41']"
          , "//lst[@name='f4_ws']/int[@name='four_6'][.='9']"
          , "//lst[@name='f5_ws']/int[@name='five_1'][.='42']"
          , "//lst[@name='f5_ws']/int[@name='five_7'][.='8']"
          , "//lst[@name='f6_ws']/int[@name='six_1'][.='43']"
          , "//lst[@name='f6_ws']/int[@name='six_8'][.='7']"
          , "//lst[@name='f7_ws']/int[@name='seven_1'][.='44']"
          , "//lst[@name='f7_ws']/int[@name='seven_9'][.='6']"
          , "//lst[@name='f8_ws']/int[@name='eight_1'][.='45']"
          , "//lst[@name='f8_ws']/int[@name='eight_10'][.='5']"
          , "//lst[@name='f9_ws']/int[@name='nine_1'][.='45']"
          , "//lst[@name='f9_ws']/int[@name='nine_11'][.='5']"

      );
      assertQ("check threading, fewer threads than fields",
          req("q", "id:*", "indent", "true", "fl", "id", "rows", "1"
              , "facet", "true"
              , "facet.field", "f0_ws"
              , "facet.field", "f1_ws"
              , "facet.field", "f2_ws"
              , "facet.field", "f3_ws"
              , "facet.field", "f4_ws"
              , "facet.field", "f5_ws"
              , "facet.field", "f6_ws"
              , "facet.field", "f7_ws"
              , "facet.field", "f8_ws"
              , "facet.field", "f9_ws"
              , "facet.threads", "3"
              , "facet.limit", "-1"
          )
          , "*[count(//lst[@name='facet_fields']/lst)=10]"
          , "*[count(//lst[@name='facet_fields']/lst/int)=20]"
          , "//lst[@name='f0_ws']/int[@name='zero_1'][.='25']"
          , "//lst[@name='f0_ws']/int[@name='zero_2'][.='25']"
          , "//lst[@name='f1_ws']/int[@name='one_1'][.='33']"
          , "//lst[@name='f1_ws']/int[@name='one_3'][.='17']"
          , "//lst[@name='f2_ws']/int[@name='two_1'][.='37']"
          , "//lst[@name='f2_ws']/int[@name='two_4'][.='13']"
          , "//lst[@name='f3_ws']/int[@name='three_1'][.='40']"
          , "//lst[@name='f3_ws']/int[@name='three_5'][.='10']"
          , "//lst[@name='f4_ws']/int[@name='four_1'][.='41']"
          , "//lst[@name='f4_ws']/int[@name='four_6'][.='9']"
          , "//lst[@name='f5_ws']/int[@name='five_1'][.='42']"
          , "//lst[@name='f5_ws']/int[@name='five_7'][.='8']"
          , "//lst[@name='f6_ws']/int[@name='six_1'][.='43']"
          , "//lst[@name='f6_ws']/int[@name='six_8'][.='7']"
          , "//lst[@name='f7_ws']/int[@name='seven_1'][.='44']"
          , "//lst[@name='f7_ws']/int[@name='seven_9'][.='6']"
          , "//lst[@name='f8_ws']/int[@name='eight_1'][.='45']"
          , "//lst[@name='f8_ws']/int[@name='eight_10'][.='5']"
          , "//lst[@name='f9_ws']/int[@name='nine_1'][.='45']"
          , "//lst[@name='f9_ws']/int[@name='nine_11'][.='5']"

      );

      // After this all, the uninverted fields should be exactly the same as they were the first time, even if we
      // blast a whole bunch of identical fields at the facet code.
      // The way fetching the uninverted field is written, all this is really testing is if the cache is working.
      // It's NOT testing whether the pending/sleep is actually functioning, I had to do that by hand since I don't
      // see how to make sure that uninverting the field multiple times actually happens to hit the wait state.
      assertQ("check threading, more threads than fields",
          req("q", "id:*", "indent", "true", "fl", "id", "rows", "1"
              , "facet", "true"
              , "facet.field", "f0_ws"
              , "facet.field", "f0_ws"
              , "facet.field", "f0_ws"
              , "facet.field", "f0_ws"
              , "facet.field", "f0_ws"
              , "facet.field", "f1_ws"
              , "facet.field", "f1_ws"
              , "facet.field", "f1_ws"
              , "facet.field", "f1_ws"
              , "facet.field", "f1_ws"
              , "facet.field", "f2_ws"
              , "facet.field", "f2_ws"
              , "facet.field", "f2_ws"
              , "facet.field", "f2_ws"
              , "facet.field", "f2_ws"
              , "facet.field", "f3_ws"
              , "facet.field", "f3_ws"
              , "facet.field", "f3_ws"
              , "facet.field", "f3_ws"
              , "facet.field", "f3_ws"
              , "facet.field", "f4_ws"
              , "facet.field", "f4_ws"
              , "facet.field", "f4_ws"
              , "facet.field", "f4_ws"
              , "facet.field", "f4_ws"
              , "facet.field", "f5_ws"
              , "facet.field", "f5_ws"
              , "facet.field", "f5_ws"
              , "facet.field", "f5_ws"
              , "facet.field", "f5_ws"
              , "facet.field", "f6_ws"
              , "facet.field", "f6_ws"
              , "facet.field", "f6_ws"
              , "facet.field", "f6_ws"
              , "facet.field", "f6_ws"
              , "facet.field", "f7_ws"
              , "facet.field", "f7_ws"
              , "facet.field", "f7_ws"
              , "facet.field", "f7_ws"
              , "facet.field", "f7_ws"
              , "facet.field", "f8_ws"
              , "facet.field", "f8_ws"
              , "facet.field", "f8_ws"
              , "facet.field", "f8_ws"
              , "facet.field", "f8_ws"
              , "facet.field", "f9_ws"
              , "facet.field", "f9_ws"
              , "facet.field", "f9_ws"
              , "facet.field", "f9_ws"
              , "facet.field", "f9_ws"
              , "facet.threads", "1000"
              , "facet.limit", "-1"
          )
          , "*[count(//lst[@name='facet_fields']/lst)=10]"
          , "*[count(//lst[@name='facet_fields']/lst/int)=20]"
      );
    } finally {
      currentSearcherRef.decref();
    }
  }
}

