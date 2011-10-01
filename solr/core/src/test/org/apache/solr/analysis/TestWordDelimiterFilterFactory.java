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

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * New WordDelimiterFilter tests... most of the tests are in ConvertedLegacyTest
 */
public class TestWordDelimiterFilterFactory extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  public void posTst(String v1, String v2, String s1, String s2) {
    assertU(adoc("id",  "42",
                 "subword", v1,
                 "subword", v2));
    assertU(commit());

    // there is a positionIncrementGap of 100 between field values, so
    // we test if that was maintained.
    assertQ("position increment lost",
            req("+id:42 +subword:\"" + s1 + ' ' + s2 + "\"~90")
            ,"//result[@numFound=0]"
    );
    assertQ("position increment lost",
            req("+id:42 +subword:\"" + s1 + ' ' + s2 + "\"~110")
            ,"//result[@numFound=1]"
    );
    clearIndex();
  }

  @Test
  public void testRetainPositionIncrement() {
    posTst("foo","bar","foo","bar");
    posTst("-foo-","-bar-","foo","bar");
    posTst("foo","bar","-foo-","-bar-");

    posTst("123","456","123","456");
    posTst("/123/","/456/","123","456");

    posTst("/123/abc","qwe/456/","abc","qwe");

    posTst("zoo-foo","bar-baz","foo","bar");
    posTst("zoo-foo-123","456-bar-baz","foo","bar");
  }

  @Test
  public void testNoGenerationEdgeCase() {
    assertU(adoc("id", "222", "numberpartfail", "123.123.123.123"));
    clearIndex();
  }

  @Test
  public void testIgnoreCaseChange() {

    assertU(adoc("id",  "43",
                 "wdf_nocase", "HellO WilliAM",
                 "subword", "GoodBye JonEs"));
    assertU(commit());
    
    assertQ("no case change",
            req("wdf_nocase:(hell o am)")
            ,"//result[@numFound=0]"
    );
    assertQ("case change",
            req("subword:(good jon)")
            ,"//result[@numFound=1]"
    );
    clearIndex();
  }

  @Test
  public void testPreserveOrignalTrue() {

    assertU(adoc("id",  "144",
                 "wdf_preserve", "404-123"));
    assertU(commit());
    
    assertQ("preserving original word",
            req("wdf_preserve:404")
            ,"//result[@numFound=1]"
    );
    
    assertQ("preserving original word",
        req("wdf_preserve:123")
        ,"//result[@numFound=1]"
    );

    assertQ("preserving original word",
        req("wdf_preserve:404-123*")
        ,"//result[@numFound=1]"
    );
    clearIndex();
  }

  /***
  public void testPerformance() throws IOException {
    String s = "now is the time-for all good men to come to-the aid of their country.";
    Token tok = new Token();
    long start = System.currentTimeMillis();
    int ret=0;
    for (int i=0; i<1000000; i++) {
      StringReader r = new StringReader(s);
      TokenStream ts = new WhitespaceTokenizer(r);
      ts = new WordDelimiterFilter(ts, 1,1,1,1,0);

      while (ts.next(tok) != null) ret++;
    }

    System.out.println("ret="+ret+" time="+(System.currentTimeMillis()-start));
  }
  ***/

  @Test
  public void testAlphaNumericWords(){
     assertU(adoc("id",  "68","numericsubword","Java/J2SE"));
     assertU(commit());

     assertQ("j2se found",
            req("numericsubword:(J2SE)")
            ,"//result[@numFound=1]"
    );
      assertQ("no j2 or se",
            req("numericsubword:(J2 OR SE)")
            ,"//result[@numFound=0]"
    );
    clearIndex();
  }

  @Test
  public void testProtectedWords(){
    assertU(adoc("id", "70","protectedsubword","c# c++ .net Java/J2SE"));
    assertU(commit());

    assertQ("java found",
            req("protectedsubword:(java)")
            ,"//result[@numFound=1]"
    );

    assertQ(".net found",
            req("protectedsubword:(.net)")
            ,"//result[@numFound=1]"
    );

    assertQ("c# found",
            req("protectedsubword:(c#)")
            ,"//result[@numFound=1]"
    );

    assertQ("c++ found",
            req("protectedsubword:(c++)")
            ,"//result[@numFound=1]"
    );

    assertQ("c found?",
            req("protectedsubword:c")
            ,"//result[@numFound=0]"
    );
    assertQ("net found?",
            req("protectedsubword:net")
            ,"//result[@numFound=0]"
    );
    clearIndex();
  }
  
  @Test
  public void testCustomTypes() throws Exception {
    String testText = "I borrowed $5,400.00 at 25% interest-rate";
    WordDelimiterFilterFactory factoryDefault = new WordDelimiterFilterFactory();
    ResourceLoader loader = new SolrResourceLoader(null, null);
    Map<String,String> args = new HashMap<String,String>();
    args.put("generateWordParts", "1");
    args.put("generateNumberParts", "1");
    args.put("catenateWords", "1");
    args.put("catenateNumbers", "1");
    args.put("catenateAll", "0");
    args.put("splitOnCaseChange", "1");
    
    /* default behavior */
    factoryDefault.init(args);
    factoryDefault.inform(loader);
    
    TokenStream ts = factoryDefault.create(
        new MockTokenizer(new StringReader(testText), MockTokenizer.WHITESPACE, false));
    BaseTokenTestCase.assertTokenStreamContents(ts, 
        new String[] { "I", "borrowed", "5", "400", "00", "540000", "at", "25", "interest", "rate", "interestrate" });

    ts = factoryDefault.create(
        new MockTokenizer(new StringReader("foo\u200Dbar"), MockTokenizer.WHITESPACE, false));
    BaseTokenTestCase.assertTokenStreamContents(ts, 
        new String[] { "foo", "bar", "foobar" });

    
    /* custom behavior */
    WordDelimiterFilterFactory factoryCustom = new WordDelimiterFilterFactory();
    // use a custom type mapping
    args.put("types", "wdftypes.txt");
    factoryCustom.init(args);
    factoryCustom.inform(loader);
    
    ts = factoryCustom.create(
        new MockTokenizer(new StringReader(testText), MockTokenizer.WHITESPACE, false));
    BaseTokenTestCase.assertTokenStreamContents(ts, 
        new String[] { "I", "borrowed", "$5,400.00", "at", "25%", "interest", "rate", "interestrate" });
    
    /* test custom behavior with a char > 0x7F, because we had to make a larger byte[] */
    ts = factoryCustom.create(
        new MockTokenizer(new StringReader("foo\u200Dbar"), MockTokenizer.WHITESPACE, false));
    BaseTokenTestCase.assertTokenStreamContents(ts, 
        new String[] { "foo\u200Dbar" });
  }
}
