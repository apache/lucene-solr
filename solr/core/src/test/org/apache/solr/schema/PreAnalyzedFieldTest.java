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
package org.apache.solr.schema;

import java.util.Collections;
import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class PreAnalyzedFieldTest extends SolrTestCaseJ4 {
  
  private static final String[] valid = {
    "1 one two three",                       // simple parsing
    "1  one  two   three ",                  // spurious spaces
    "1 one,s=123,e=128,i=22  two three,s=20,e=22,y=foobar",    // attribs
    "1 \\ one\\ \\,,i=22,a=\\, two\\=\n\r\t\\n,\\ =\\   \\", // escape madness
    "1 ,i=22 ,i=33,s=2,e=20 , ", // empty token text, non-empty attribs
    "1 =This is the stored part with \\= \n \\n \t \\t escapes.=one two three  \u0001ąćęłńóśźż", // stored plus token stream
    "1 ==", // empty stored, no token stream
    "1 =this is a test.=", // stored + empty token stream
    "1 one,p=deadbeef two,p=0123456789abcdef three" // payloads
  };
  
  private static final String[] validParsed = {
    "1 one,s=0,e=3 two,s=4,e=7 three,s=8,e=13",
    "1 one,s=1,e=4 two,s=6,e=9 three,s=12,e=17",
    "1 one,i=22,s=123,e=128,y=word two,i=1,s=5,e=8,y=word three,i=1,s=20,e=22,y=foobar",
    "1 \\ one\\ \\,,i=22,s=0,e=6 two\\=\\n\\r\\t\\n,i=1,s=7,e=15 \\\\,i=1,s=17,e=18",
    "1 i=22,s=0,e=0 i=33,s=2,e=20 i=1,s=2,e=2",
    "1 =This is the stored part with = \n \\n \t \\t escapes.=one,s=0,e=3 two,s=4,e=7 three,s=8,e=13 \u0001ąćęłńóśźż,s=15,e=25",
    "1 ==",
    "1 =this is a test.=",
    "1 one,p=deadbeef,s=0,e=3 two,p=0123456789abcdef,s=4,e=7 three,s=8,e=13"
  };

  private static final String[] invalidSimple = {
    "one two three", // missing version #
    "2 one two three", // invalid version #
    "1 o,ne two", // missing escape
    "1 one t=wo", // missing escape
    "1 one,, two", // missing attribs, unescaped comma
    "1 one,s ",   // missing attrib value
    "1 one,s= val", // missing attrib value, unescaped space
    "1 one,s=,val", // unescaped comma
    "1 =", // unescaped equals
    "1 =stored ", // unterminated stored
    "1 ===" // empty stored (ok), but unescaped = in token stream
  };

  private static final String validJson
      = json("{'v':'1','str':'stored-value','tokens':[{'t':'a'},{'t':'b'},{'t':'c'}]}");

  private static final String[] invalidJson = {
      json("'v':'1','str':'stored-value','tokens':[{'t':'a'},{'t':'b'},{'t':'c'}]"),    // missing enclosing object
      json("{'str':'stored-value','tokens':[{'t':'a'},{'t':'b'},{'t':'c'}]}"),          // missing version #
      json("{'v':'2','str':'stored-value','tokens':[{'t':'a'},{'t':'b'},{'t':'c'}]}"),  // invalid version #
      json("{'v':'1','str':'stored-value','tokens':[{}]}"),                             // single token no attribs
      json("{'v':'1','str':'stored-value','tokens':[{'t'}]}"),                          // missing attrib value
  };

  SchemaField field = null;
  int props = 
    FieldProperties.INDEXED | FieldProperties.STORED;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml","schema-preanalyzed.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    field = new SchemaField("content", new TextField(), props, null);
  }
  
  @Test
  public void testValidSimple() {
    PreAnalyzedField paf = new PreAnalyzedField();
    // use Simple format
    HashMap<String,String> args = new HashMap<>();
    args.put(PreAnalyzedField.PARSER_IMPL, SimplePreAnalyzedParser.class.getName());
    paf.init(h.getCore().getLatestSchema(), args);
    PreAnalyzedParser parser = new SimplePreAnalyzedParser();
    for (int i = 0; i < valid.length; i++) {
      String s = valid[i];
      try {
        Field f = (Field)paf.fromString(field, s);
        //System.out.println(" - toString: '" + sb.toString() + "'");
        assertEquals(validParsed[i], parser.toFormattedString(f));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Should pass: '" + s + "', exception: " + e);
      }
    }
  }

  private String addTwoDocs(int firstId, String field) {
    return "<add>\n"
        + doc("id", Integer.toString(firstId), field,
              json("{'v':'1','str':'document one','tokens':[{'t':'one'},{'t':'two'},{'t':'three','i':100}]}"))
        + doc("id", Integer.toString(firstId + 1), field,
              json("{'v':'1','str':'document two','tokens':[{'t':'eleven'},{'t':'twelve'},{'t':'thirteen','i':110}]}"))
        + "</add>\n";
  }

  @Test
  public void testIndexAndQueryNoSchemaAnalyzer() throws Exception {
    assertU(addTwoDocs(1, "pre_no_analyzer"));
    assertU(commit());
    assertQ(req("q", "id:(1 2)", "sort", "id asc")
        ,"//result[@numFound='2']"
        ,"//result/doc[1]/str[@name='id'][.='1']"
        ,"//result/doc[1]/str[@name='pre_no_analyzer'][.='document one']"
        ,"//result/doc[2]/str[@name='id'][.='2']"
        ,"//result/doc[2]/str[@name='pre_no_analyzer'][.='document two']"
    );
    assertQ(req("q", "{!field f='pre_no_analyzer'}{'v':'1','tokens':[{'t':'two'}]}")
        ,"//result[@numFound='1']"
    );
    assertQ(req("q", "{!field f='pre_no_analyzer'}{'v':'1','tokens':[{'t':'eleven'},{'t':'twelve'}]}")
        ,"//result[@numFound='1']"
    );
  }

  @Test
  public void testIndexAndQueryWithSchemaAnalyzer() {
    assertU(addTwoDocs(3, "pre_with_analyzer"));
    assertU(commit());
    assertQ(req("q", "id:(3 4)", "sort", "id asc")
        ,"//result[@numFound='2']"
        ,"//result/doc[1]/str[@name='id'][.='3']"
        ,"//result/doc[1]/str[@name='pre_with_analyzer'][.='document one']"
        ,"//result/doc[2]/str[@name='id'][.='4']"
        ,"//result/doc[2]/str[@name='pre_with_analyzer'][.='document two']"
    );
    assertQ(req("q", "pre_with_analyzer:(+two +three)"), "//result[@numFound='1']");
    assertQ(req("q", "pre_with_analyzer:(+eleven +twelve)"), "//result[@numFound='1']");
  }

  @Test
  public void testIndexAndQueryWithSchemaQueryAnalyzer() {
    assertU(addTwoDocs(5, "pre_with_query_analyzer"));
    assertU(commit());
    assertQ(req("q", "id:(5 6)", "sort", "id asc")
        ,"//result[@numFound='2']"
        ,"//result/doc[1]/str[@name='id'][.='5']"
        ,"//result/doc[1]/str[@name='pre_with_query_analyzer'][.='document one']"
        ,"//result/doc[2]/str[@name='id'][.='6']"
        ,"//result/doc[2]/str[@name='pre_with_query_analyzer'][.='document two']"
    );
    assertQ(req("q", "pre_with_query_analyzer:one,two"), "//result[@numFound='1']");
    assertQ(req("q", "pre_with_query_analyzer:eleven,twelve"), "//result[@numFound='1']");
  }

  @Test
  public void testInvalidSimple() {
    PreAnalyzedField paf = new PreAnalyzedField();
    paf.init(h.getCore().getLatestSchema(), Collections.<String,String>emptyMap());
    for (String s : invalidSimple) {
      try {
        paf.fromString(field, s);
        fail("should fail: '" + s + "'");
      } catch (Exception e) {
        //
      }
    }
  }

  public void testInvalidJson() throws Exception {
    PreAnalyzedField paf = new PreAnalyzedField();
    paf.init(h.getCore().getLatestSchema(), Collections.emptyMap());
    Analyzer preAnalyzer = paf.getIndexAnalyzer();
    for (String s: invalidJson) {
      TokenStream stream = null;
      try {
        stream = preAnalyzer.tokenStream("dummy", s);
        stream.reset(); // exception should be triggered here.
        fail("should fail: '" + s + "'");
      } catch (Exception e) {
        // expected
      } finally {
        if (stream != null) {
          stream.close();
        }
      }
    }
    // make sure the analyzer can now handle properly formatted input
    TokenStream stream = preAnalyzer.tokenStream("dummy", validJson);
    CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);
    stream.reset();
    while (stream.incrementToken()) {
      assertFalse("zero-length token", termAttr.length() == 0);
    }
    stream.end();
    stream.close();
  }
  
  // "1 =test ąćęłńóśźż \u0001=one,i=22,s=123,e=128,p=deadbeef,y=word two,i=1,s=5,e=8,y=word three,i=1,s=20,e=22,y=foobar"
  
  private static final String jsonValid = "{\"v\":\"1\",\"str\":\"test ąćęłńóśźż\",\"tokens\":[" +
      "{\"e\":128,\"i\":22,\"p\":\"DQ4KDQsODg8=\",\"s\":123,\"t\":\"one\",\"y\":\"word\"}," +
      "{\"e\":8,\"i\":1,\"s\":5,\"t\":\"two\",\"y\":\"word\"}," +
      "{\"e\":22,\"i\":1,\"s\":20,\"t\":\"three\",\"y\":\"foobar\"}" +
      "]}";
  
  @Test
  public void testParsers() throws Exception {
    PreAnalyzedField paf = new PreAnalyzedField();
    // use Simple format
    HashMap<String,String> args = new HashMap<>();
    args.put(PreAnalyzedField.PARSER_IMPL, SimplePreAnalyzedParser.class.getName());
    paf.init(h.getCore().getLatestSchema(), args);
    {
      Field f = (Field)paf.fromString(field, valid[0]);
    }

    // use JSON format
    args.put(PreAnalyzedField.PARSER_IMPL, JsonPreAnalyzedParser.class.getName());
    paf.init(h.getCore().getLatestSchema(), args);
    expectThrows(Exception.class, () -> paf.fromString(field, valid[0]));

    byte[] deadbeef = new byte[]{(byte)0xd, (byte)0xe, (byte)0xa, (byte)0xd, (byte)0xb, (byte)0xe, (byte)0xe, (byte)0xf};
    PreAnalyzedParser parser = new JsonPreAnalyzedParser();

    {
      Field f = (Field)paf.fromString(field, jsonValid);
      assertEquals(jsonValid, parser.toFormattedString(f));
    }
  }
}
