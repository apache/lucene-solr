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

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.lucene.util.mutable.MutableValueDouble;
import org.apache.lucene.util.mutable.MutableValueFloat;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.PointMerger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TermsComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTest() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  @Before
  public void createIndex() {
    // cleanup first
    assertU(delQ("*:*"));
    assertU(commit());

    assertU(adoc("id", "0", "lowerfilt", "a", "standardfilt", "a", "foo_i", "1"));
    assertU(adoc("id", "1", "lowerfilt", "a", "standardfilt", "aa", "foo_i","1"));
    assertU(adoc("id", "2", "lowerfilt", "aa", "standardfilt", "aaa", "foo_i","2"));
    assertU(adoc("id", "3", "lowerfilt", "aaa", "standardfilt", "abbb"));
    assertU(adoc("id", "4", "lowerfilt", "ab", "standardfilt", "b"));
    assertU(adoc("id", "5", "lowerfilt", "abb", "standardfilt", "bb"));
    assertU(adoc("id", "6", "lowerfilt", "abc", "standardfilt", "bbbb"));
    assertU(adoc("id", "7", "lowerfilt", "b", "standardfilt", "c"));
    assertU(adoc("id", "8", "lowerfilt", "baa", "standardfilt", "cccc"));
    assertU(adoc("id", "9", "lowerfilt", "bbb", "standardfilt", "ccccc"));


    assertU(adoc("id", "10", "standardfilt", "ddddd"));
    assertU(commit());

    assertU(adoc("id", "11", "standardfilt", "ddddd"));
    assertU(adoc("id", "12", "standardfilt", "ddddd"));
    assertU(adoc("id", "13", "standardfilt", "ddddd"));
    assertU(adoc("id", "14", "standardfilt", "d"));
    assertU(adoc("id", "15", "standardfilt", "d"));
    assertU(adoc("id", "16", "standardfilt", "d"));

    assertU(commit());

    assertU(adoc("id", "17", "standardfilt", "snake"));
    assertU(adoc("id", "18", "standardfilt", "spider"));
    assertU(adoc("id", "19", "standardfilt", "shark"));
    assertU(adoc("id", "20", "standardfilt", "snake"));
    assertU(adoc("id", "21", "standardfilt", "snake"));
    assertU(adoc("id", "22", "standardfilt", "shark"));
    assertU(adoc("id", "23", "standardfilt", "a,b"));

    assertU(commit());
  }

  @Test
  public void testEmptyLower() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true", "terms.fl","lowerfilt", "terms.upper","b")
        ,"count(//lst[@name='lowerfilt']/*)=6"
        ,"//int[@name='a'] "
        ,"//int[@name='aa'] "
        ,"//int[@name='aaa'] "
        ,"//int[@name='ab'] "
        ,"//int[@name='abb'] "
        ,"//int[@name='abc'] "
    );
  }


  @Test
  public void testMultipleFields() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt", "terms.upper","b",
        "terms.fl","standardfilt")
        ,"count(//lst[@name='lowerfilt']/*)=6"
        ,"count(//lst[@name='standardfilt']/*)=5"
    );

  }

  @Test
  public void testUnlimitedRows() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt",
        "terms.fl","standardfilt")
        ,"count(//lst[@name='lowerfilt']/*)=9"
        ,"count(//lst[@name='standardfilt']/*)=10"
    );
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt",
        "terms.fl","standardfilt",
        "terms.limit","-1")
        ,"count(//lst[@name='lowerfilt']/*)=9"
        ,"count(//lst[@name='standardfilt']/*)=16"
    );


  }

  @Test
  public void testPrefix() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt", "terms.upper","b",
        "terms.fl","standardfilt",
        "terms.lower","aa", "terms.lower.incl","false", "terms.prefix","aa", "terms.upper","b", "terms.limit","50")
        ,"count(//lst[@name='lowerfilt']/*)=1"
        ,"count(//lst[@name='standardfilt']/*)=1"
    );
  }

  @Test
  public void testRegexp() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","a", "terms.lower.incl","false",
        "terms.upper","c", "terms.upper.incl","true",
        "terms.regex","b.*")
        ,"count(//lst[@name='standardfilt']/*)=3"        
    );
  }

  @Test
  public void testRegexpFlagParsing() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(TermsParams.TERMS_REGEXP_FLAG, "case_insensitive", "literal", "comments", "multiline", "unix_lines",
              "unicode_case", "dotall", "canon_eq");
      int flags = new TermsComponent().resolveRegexpFlags(params);
      int expected = Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.COMMENTS | Pattern.MULTILINE | Pattern.UNIX_LINES
              | Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CANON_EQ;
      assertEquals(expected, flags);
  }

  @Test
  public void testRegexpWithFlags() throws Exception {
    // TODO: there are no uppercase or mixed-case terms in the index!
    assertQ(req("indent", "true", "qt", "/terms", "terms", "true",
            "terms.fl", "standardfilt",
            "terms.lower", "a", "terms.lower.incl", "false",
            "terms.upper", "c", "terms.upper.incl", "true",
            "terms.regex", "B.*",
            "terms.regex.flag", "case_insensitive")
        , "count(//lst[@name='standardfilt']/*)=3"
    );
  }

  @Test
  public void testSortCount() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","s", "terms.lower.incl","false",
        "terms.prefix","s",
        "terms.sort","count")
        ,"count(//lst[@name='standardfilt']/*)=3"
        ,"//lst[@name='standardfilt']/int[1][@name='snake'][.='3']"
        ,"//lst[@name='standardfilt']/int[2][@name='shark'][.='2']"
        ,"//lst[@name='standardfilt']/int[3][@name='spider'][.='1']"
    );
  
  }

  @Test
  public void testTermsList() throws Exception {
    //Terms list always returns in index order
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
            "terms.fl","standardfilt",
            "terms.list","spider,snake,a\\,b,shark,ddddd,bad")
        ,"count(//lst[@name='standardfilt']/*)=5"
        ,"//lst[@name='standardfilt']/int[1][@name='a,b'][.='1']"
        ,"//lst[@name='standardfilt']/int[2][@name='ddddd'][.='4']"
        ,"//lst[@name='standardfilt']/int[3][@name='shark'][.='2']"
        ,"//lst[@name='standardfilt']/int[4][@name='snake'][.='3']"
        ,"//lst[@name='standardfilt']/int[5][@name='spider'][.='1']"
    );


    //Test with numeric terms
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
            "terms.fl","foo_i",
            "terms.list","2,1")
        ,"count(//lst[@name='foo_i']/*)=2"
        ,"//lst[@name='foo_i']/int[1][@name='1'][.='2']"
        ,"//lst[@name='foo_i']/int[2][@name='2'][.='1']"
    );
  }


  @Test
  public void testStats() throws Exception {
    //Terms list always returns in index order
    assertQ(req("indent", "true", "qt", "/terms", "terms", "true",
            "terms.fl", "standardfilt","terms.stats", "true",
            "terms.list", "spider,snake,shark,ddddd,bad")
        , "//lst[@name='indexstats']/long[1][@name='numDocs'][.='24']"
    );
  }

  @Test
  public void testSortIndex() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","s", "terms.lower.incl","false",
        "terms.prefix","s",
        "terms.sort","index")
        ,"count(//lst[@name='standardfilt']/*)=3"
        ,"//lst[@name='standardfilt']/int[1][@name='shark'][.='2']"
        ,"//lst[@name='standardfilt']/int[2][@name='snake'][.='3']"
        ,"//lst[@name='standardfilt']/int[3][@name='spider'][.='1']"
    );
  }
  
  @Test
  public void testPastUpper() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt",
        //no upper bound, lower bound doesn't exist
        "terms.lower","d")
        ,"count(//lst[@name='standardfilt']/*)=0"
    );
  }

  @Test
  public void testLowerExclusive() throws Exception {
     assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","lowerfilt",
        "terms.lower","a", "terms.lower.incl","false",
        "terms.upper","b")
        ,"count(//lst[@name='lowerfilt']/*)=5"
        ,"//int[@name='aa'] "
        ,"//int[@name='aaa'] "
        ,"//int[@name='ab'] "
        ,"//int[@name='abb'] "
        ,"//int[@name='abc'] "
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
        "terms.fl","standardfilt",
        "terms.lower","cc", "terms.lower.incl","false",
        "terms.upper","d")
        ,"count(//lst[@name='standardfilt']/*)=2"
    );
  }

  @Test
  public void test() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","lowerfilt",
       "terms.lower","a",
       "terms.upper","b")
       ,"count(//lst[@name='lowerfilt']/*)=6"
       ,"//int[@name='a'] "
       ,"//int[@name='aa'] "
       ,"//int[@name='aaa'] "
       ,"//int[@name='ab'] "
       ,"//int[@name='abb'] "
       ,"//int[@name='abc'] "
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","lowerfilt",
       "terms.lower","a",
       "terms.upper","b",
       "terms.raw","true",    // this should have no effect on a text field
       "terms.limit","2")
       ,"count(//lst[@name='lowerfilt']/*)=2"
       ,"//int[@name='a']"
       ,"//int[@name='aa']"
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","foo_i")
       ,"//int[@name='1'][.='2']"
    );

    /* terms.raw only applies to indexed fields
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","foo_i", "terms.raw","true")
       ,"not(//int[@name='1'][.='2'])"
    );
    */

    // check something at the end of the index
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","zzz_i")
        ,"count(//lst[@name='zzz_i']/*)=0"
    );
  }

  @Test
  public void testMinMaxFreq() throws Exception {
    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","lowerfilt",
       "terms.lower","a",
       "terms.mincount","2",
       "terms.maxcount","-1",
       "terms.limit","50")
       ,"count(//lst[@name='lowerfilt']/*)=1"
    );

    assertQ(req("indent","true", "qt","/terms",  "terms","true",
       "terms.fl","standardfilt",
       "terms.lower","d",
       "terms.mincount","2",
       "terms.maxcount","3",
       "terms.limit","50")
       ,"count(//lst[@name='standardfilt']/*)=3"
    );
  }

  @Test
  public void testDocFreqAndTotalTermFreq() throws Exception {
    SolrQueryRequest req = req(
        "indent","true",
        "qt", "/terms",
        "terms", "true",
        "terms.fl", "standardfilt",
        "terms.ttf", "true",
        "terms.list", "snake,spider,shark,ddddd");
    assertQ(req,
        "count(//lst[@name='standardfilt']/*)=4",
        "//lst[@name='standardfilt']/lst[@name='ddddd']/long[@name='df'][.='4']",
        "//lst[@name='standardfilt']/lst[@name='ddddd']/long[@name='ttf'][.='4']",
        "//lst[@name='standardfilt']/lst[@name='shark']/long[@name='df'][.='2']",
        "//lst[@name='standardfilt']/lst[@name='shark']/long[@name='ttf'][.='2']",
        "//lst[@name='standardfilt']/lst[@name='snake']/long[@name='df'][.='3']",
        "//lst[@name='standardfilt']/lst[@name='snake']/long[@name='ttf'][.='3']",
        "//lst[@name='standardfilt']/lst[@name='spider']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='spider']/long[@name='ttf'][.='1']");

    // terms.limit=-1 and terms.sort=count and NO terms.list
    req = req(
        "indent","true",
        "qt", "/terms",
        "terms", "true",
        "terms.fl", "standardfilt",
        "terms.ttf", "true",
        "terms.limit", "-1",
        "terms.sort", "count"
        );
    assertQ(req,
        "count(//lst[@name='standardfilt']/*)>=4", // it would be at-least 4
        "//lst[@name='standardfilt']/lst[@name='ddddd']/long[@name='df'][.='4']",
        "//lst[@name='standardfilt']/lst[@name='ddddd']/long[@name='ttf'][.='4']",
        "//lst[@name='standardfilt']/lst[@name='shark']/long[@name='df'][.='2']",
        "//lst[@name='standardfilt']/lst[@name='shark']/long[@name='ttf'][.='2']",
        "//lst[@name='standardfilt']/lst[@name='snake']/long[@name='df'][.='3']",
        "//lst[@name='standardfilt']/lst[@name='snake']/long[@name='ttf'][.='3']",
        "//lst[@name='standardfilt']/lst[@name='spider']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='spider']/long[@name='ttf'][.='1']");
  }

  @Test
  public void testDocFreqAndTotalTermFreqForNonExistingTerm() throws Exception {
    SolrQueryRequest req = req(
        "indent","true",
        "qt", "/terms",
        "terms", "true",
        "terms.fl", "standardfilt",
        "terms.ttf", "true",
        "terms.list", "boo,snake");
    assertQ(req,
        "count(//lst[@name='standardfilt']/*)=1",
        "//lst[@name='standardfilt']/lst[@name='snake']/long[@name='df'][.='3']",
        "//lst[@name='standardfilt']/lst[@name='snake']/long[@name='ttf'][.='3']");
  }

  @Test
  public void testDocFreqAndTotalTermFreqForMultipleFields() throws Exception {
    SolrQueryRequest req = req(
        "indent","true",
        "qt", "/terms",
        "terms", "true",
        "terms.fl", "lowerfilt",
        "terms.fl", "standardfilt",
        "terms.ttf", "true",
        "terms.list", "a,aa,aaa");
    assertQ(req,
        "count(//lst[@name='lowerfilt']/*)=3",
        "count(//lst[@name='standardfilt']/*)=3",
        "//lst[@name='lowerfilt']/lst[@name='a']/long[@name='df'][.='2']",
        "//lst[@name='lowerfilt']/lst[@name='a']/long[@name='ttf'][.='2']",
        "//lst[@name='lowerfilt']/lst[@name='aa']/long[@name='df'][.='1']",
        "//lst[@name='lowerfilt']/lst[@name='aa']/long[@name='ttf'][.='1']",
        "//lst[@name='lowerfilt']/lst[@name='aaa']/long[@name='df'][.='1']",
        "//lst[@name='lowerfilt']/lst[@name='aaa']/long[@name='ttf'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='a']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='a']/long[@name='ttf'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aa']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aa']/long[@name='ttf'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aaa']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aaa']/long[@name='ttf'][.='1']");

    // terms.ttf=true, terms.sort=index and no terms list
    req = req(
        "indent","true",
        "qt", "/terms",
        "terms", "true",
        "terms.fl", "lowerfilt",
        "terms.fl", "standardfilt",
        "terms.ttf", "true",
        "terms.sort", "index",
        "terms.limit", "10"
        );
    assertQ(req,
        "count(//lst[@name='lowerfilt']/*)<=10",
        "count(//lst[@name='standardfilt']/*)<=10",
        "//lst[@name='lowerfilt']/lst[@name='a']/long[@name='df'][.='2']",
        "//lst[@name='lowerfilt']/lst[@name='a']/long[@name='ttf'][.='2']",
        "//lst[@name='lowerfilt']/lst[@name='aa']/long[@name='df'][.='1']",
        "//lst[@name='lowerfilt']/lst[@name='aa']/long[@name='ttf'][.='1']",
        "//lst[@name='lowerfilt']/lst[@name='aaa']/long[@name='df'][.='1']",
        "//lst[@name='lowerfilt']/lst[@name='aaa']/long[@name='ttf'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='a']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='a']/long[@name='ttf'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aa']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aa']/long[@name='ttf'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aaa']/long[@name='df'][.='1']",
        "//lst[@name='standardfilt']/lst[@name='aaa']/long[@name='ttf'][.='1']");
  }

  @Test
  public void testPointField() throws Exception {
    int nvals = 10000; int maxval = 1000000;
    // int nvals = 5; int maxval = 2;
    final int vals[] = new int[nvals];
    for (int i=0; i<nvals; i++) {
      vals[i] = random().nextInt(maxval);
      String v = Integer.toString(vals[i]);
      assertU(adoc("id", Integer.toString(100000+i), "foo_pi",v, "foo_pl",v, "foo_pf",v, "foo_pd",v) );
      if (random().nextInt(1000) == 0) assertU(commit());  // make multiple segments
    }

    assertU(commit());
    // assertU(optimize());

    Arrays.sort(vals);

    // find the first two values and account for dups
    int val1 = vals[0];
    int val2 = vals[1];
    for (int i=2; i<vals.length; i++) {
      if (val2 != val1) break;
      val2 = vals[i];
    }

    SolrQueryRequest req = req(
        "qt", "/terms",
        "terms", "true",
        "terms.fl", "foo_pi");
    ;
    try {
      SchemaField sf = req.getSchema().getField("foo_pi");

      /**
      LeafReader r = req.getSearcher().getIndexReader().leaves().get(0).reader();
      PointValues pv = r.getPointValues("foo_pi");
      System.out.println("pv=" + pv);
      if (pv instanceof AssertingLeafReader.AssertingPointValues) {
        pv = ((AssertingLeafReader.AssertingPointValues) pv).getWrapped();
      }
      System.out.println("pv=" + pv);
      BKDReader bkdr = (BKDReader)pv;

       for (int i=0; i<Math.min(10,nvals); i++) { System.out.println("INDEXED VAL=" + vals[i]); }
      **/


      //
      // iterate all values
      //
      int totBuff = random().nextInt(50)+1;
      int minSegBuff = random().nextInt(10)+1;
      PointMerger.ValueIterator iter = new PointMerger.ValueIterator(req.getSchema().getField("foo_pi"), req.getSearcher().getIndexReader().leaves(), totBuff, minSegBuff);
      MutableValueInt v = (MutableValueInt)iter.getMutableValue();
      int i=0;
      for (;;) {
        long count = iter.getNextCount();
        if (count < 0) break;
        assertEquals( vals[i], v.value );
        i += count;
        // if (i < 10) System.out.println("COUNT=" + count + " OBJ="+v.toObject());
      }
      assert(i==nvals);

      totBuff = random().nextInt(50)+1;
      minSegBuff = random().nextInt(10)+1;
      iter = new PointMerger.ValueIterator(req.getSchema().getField("foo_pl"), req.getSearcher().getIndexReader().leaves());
      MutableValueLong lv = (MutableValueLong)iter.getMutableValue();
      i=0;
      for (;;) {
        long count = iter.getNextCount();
        if (count < 0) break;
        assertEquals( vals[i], lv.value );
        i += count;
        // if (i < 10) System.out.println("COUNT=" + count + " OBJ="+v.toObject());
      }
      assert(i==nvals);

      totBuff = random().nextInt(50)+1;
      minSegBuff = random().nextInt(10)+1;
      iter = new PointMerger.ValueIterator(req.getSchema().getField("foo_pf"), req.getSearcher().getIndexReader().leaves());
      MutableValueFloat fv = (MutableValueFloat)iter.getMutableValue();
      i=0;
      for (;;) {
        long count = iter.getNextCount();
        if (count < 0) break;
        assertEquals( vals[i], fv.value, 0);
        i += count;
        // if (i < 10) System.out.println("COUNT=" + count + " OBJ="+v.toObject());
      }
      assert(i==nvals);

      totBuff = random().nextInt(50)+1;
      minSegBuff = random().nextInt(10)+1;
      iter = new PointMerger.ValueIterator(req.getSchema().getField("foo_pd"), req.getSearcher().getIndexReader().leaves());
      MutableValueDouble dv = (MutableValueDouble)iter.getMutableValue();
      i=0;
      for (;;) {
        long count = iter.getNextCount();
        if (count < 0) break;
        assertEquals( vals[i], dv.value, 0);
        i += count;
        // if (i < 10) System.out.println("COUNT=" + count + " OBJ="+v.toObject());
      }
      assert(i==nvals);

      assertQ(req("indent","true", "qt","/terms",  "terms","true",
          "terms.fl","foo_pi", "terms.sort","index", "terms.limit","2")
          ,"count(//lst[@name='foo_pi']/*)=2"
          ,"//lst[@name='foo_pi']/int[1][@name='" +val1+ "']"
          ,"//lst[@name='foo_pi']/int[2][@name='" +val2+ "']"
      );


    } finally {
      req.close();
      assertU(delQ("foo_pi:[* TO *]"));
      assertU(commit());
    }
  }

  @Test
  public void testTermsSortIndexDistribution() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_INDEX);
    params.set(TermsParams.TERMS_LIMIT, "any-number");
    assertEquals(params.toString(), createShardQueryParamsString(params));
    params.set(TermsParams.TERMS_MINCOUNT, "0");
    assertEquals(params.toString(), createShardQueryParamsString(params));
    params.set(TermsParams.TERMS_MINCOUNT, "1");
    assertEquals(params.toString(), createShardQueryParamsString(params));
    // include all (also lower mincount) since 2 shards can have one each
    params.set(TermsParams.TERMS_MINCOUNT, "2");
    assertNotEquals(params.toString(), createShardQueryParamsString(params));
    // "unlimited" since 2 shards can have 30 each, and term then should not be included
    params.remove(TermsParams.TERMS_MINCOUNT);
    params.set(TermsParams.TERMS_MAXCOUNT, "32");
    assertNotEquals(params.toString(), createShardQueryParamsString(params));
  }

  private static String createShardQueryParamsString(ModifiableSolrParams params) {
    return TermsComponent.createShardQuery(params).params.toString();
  }

  @Test
  public void testDatePointField() throws Exception {
    String[] dates = new String[]{"2015-01-03T14:30:00Z", "2014-03-15T12:00:00Z"};
    for (int i = 0; i < 100; i++) {
      assertU(adoc("id", Integer.toString(100000+i), "foo_pdt", dates[i % 2]) );
      if (random().nextInt(10) == 0) assertU(commit());  // make multiple segments
    }
    assertU(commit());
    assertU(adoc("id", Integer.toString(100102), "foo_pdt", dates[1]));
    assertU(commit());

    assertQ(req("indent","true", "qt","/terms", "terms","true",
        "terms.fl","foo_pdt", "terms.sort","count"),
        "count(//lst[@name='foo_pdt']/*)=2",
        "//lst[@name='foo_pdt']/int[1][@name='" + dates[1] + "'][.='51']",
        "//lst[@name='foo_pdt']/int[2][@name='" + dates[0] + "'][.='50']"
    );

    // test on empty index
    assertU(delQ("*:*"));
    assertU(commit());

    assertQ(req("indent","true", "qt","/terms", "terms","true",
        "terms.fl","foo_pdt", "terms.sort","count"),
        "count(//lst[@name='foo_pdt']/*)=0"
    );
  }
}
