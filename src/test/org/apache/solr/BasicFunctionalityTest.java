/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr;

import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using AbstractSolrTestCase
 */
public class BasicFunctionalityTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
  }
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();

  }

  public void testSomeStuff() throws Exception {

    assertQ("test query on empty index",
            req("qlkciyopsbgzyvkylsjhchghjrdf")
            ,"//result[@numFound='0']"
            );

    // test escaping of ";"
    assertU("deleting 42 for no reason at all",
            delI("42"));
    assertU("adding doc#42",
            adoc("id", "42", "val_s", "aa;bb"));
    assertU("does commit work?",
            commit());

    assertQ("backslash escaping semicolon",
            req("id:42 AND val_s:aa\\;bb")
            ,"//*[@numFound='1']"
            ,"//int[@name='id'][.='42']"
            );

    assertQ("quote escaping semicolon",
            req("id:42 AND val_s:\"aa;bb\"")
            ,"//*[@numFound='1']"
            ,"//int[@name='id'][.='42']"
            );

    assertQ("no escaping semicolon",
            req("id:42 AND val_s:aa")
            ,"//*[@numFound='0']"
            );

    assertU(delI("42"));
    assertU(commit());
    assertQ(req("id:42")
            ,"//*[@numFound='0']"
            );

    // test allowDups default of false

    assertU(adoc("id", "42", "val_s", "AAA"));
    assertU(adoc("id", "42", "val_s", "BBB"));
    assertU(commit());
    assertQ(req("id:42")
            ,"//*[@numFound='1']"
            ,"//str[.='BBB']"
            );
    assertU(adoc("id", "42", "val_s", "CCC"));
    assertU(adoc("id", "42", "val_s", "DDD"));
    assertU(commit());
    assertQ(req("id:42")
            ,"//*[@numFound='1']"
            ,"//str[.='DDD']"
            );

    // test deletes
    String [] adds = new String[] {
      add( doc("id","101"), "allowDups", "false" ),
      add( doc("id","101"), "allowDups", "false" ),
      add( doc("id","105"), "allowDups", "true"  ),
      add( doc("id","102"), "allowDups", "false" ),
      add( doc("id","103"), "allowDups", "true"  ),
      add( doc("id","101"), "allowDups", "false" ),
    };
    for (String a : adds) {
      assertU(a, a);
    }
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='4']"
            );
    assertU(delI("102"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='3']"
            );
    assertU(delI("105"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='2']"
            );
    assertU(delQ("id:[100 TO 110]"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='0']"
            );
  }


  public void testMultipleUpdatesPerAdd() {

    // big freaking kludge since the response is currently not well formed.
    String res = h.update("<add><doc><field name=\"id\">1</field></doc><doc><field name=\"id\">2</field></doc></add>");
    assertEquals("<result status=\"0\"></result><result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("id:[0 TO 99]")
            ,"//*[@numFound='2']"
            );

  }

  public void testXMLWriter() throws Exception {

    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("\"quoted\"", "\"value\"");

    StringWriter writer = new StringWriter(32000);
    XMLWriter.writeResponse(writer,req("foo"),rsp);

    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    builder.parse(new ByteArrayInputStream
                             (writer.toString().getBytes("UTF-8")));
  }

  public void testLocalSolrQueryRequestParams() {
    HashMap args = new HashMap();
    args.put("string", "string value");
    args.put("array", new String[] {"array", "value"});
    SolrQueryRequest req = new LocalSolrQueryRequest(null, null, null, 0, 20, args);
    assertEquals("string value", req.getParam("string"));
    assertEquals("array", req.getParam("array"));

    String[] stringParams = req.getParams("string");
    assertEquals(1, stringParams.length);
    assertEquals("string value", stringParams[0]);

    String[] arrayParams = req.getParams("array");
    assertEquals(2, arrayParams.length);
    assertEquals("array", arrayParams[0]);
    assertEquals("value", arrayParams[1]);
  }

  public void testKeywordTokenizerFactory() {

    assertU(adoc("id", "42",
                 "keywordtok", "How nOw broWn-ish C.o.w. ?"));
    assertU(commit());
    assertQ("stored value matches?",
            req("id:42")
            ,"//str[.='How nOw broWn-ish C.o.w. ?']"
            );
    assertQ("query on exact matches?",
            req("keywordtok:\"How nOw broWn-ish C.o.w. ?\"")
            ,"//str[.='How nOw broWn-ish C.o.w. ?']"
            );
  }


//   /** this doesn't work, but if it did, this is how we'd test it. */
//   public void testOverwriteFalse() {

//     assertU(adoc("id", "overwrite", "val_s", "AAA"));
//     assertU(commit());

//     assertU(add(doc("id", "overwrite", "val_s", "BBB")
//                 ,"allowDups", "false"
//                 ,"overwriteCommitted","false"
//                 ,"overwritePending","false"
//                 ));
//     assertU(commit());
//     assertQ(req("id:overwrite")
//             ,"//*[@numFound='1']"
//             ,"//str[.='AAA']"
//             );
//   }


}
