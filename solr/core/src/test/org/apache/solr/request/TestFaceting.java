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

import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.FacetParams;
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

    UnInvertedField uif = new UnInvertedField(proto.field(), req.getSearcher());

    assertEquals(size, uif.getNumTerms());

    TermsEnum te = uif.getOrdTermsEnum(req.getSearcher().getAtomicReader());
    assertEquals(size == 0, te == null);

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
      assertEquals(size>0, te.seekCeil(new BytesRef("000"), true) != TermsEnum.SeekStatus.END);
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
    List<String> fields = new ArrayList<String>();
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

}