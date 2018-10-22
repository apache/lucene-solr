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
package org.apache.solr.update.processor;

import org.apache.lucene.document.Field;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class PreAnalyzedUpdateProcessorTest extends UpdateProcessorTestBase {
  String[] simpleTitle = new String[] {
      "not pre-analyzed",
      "1 =string value=foo bar"
  };
  String[] jsonTitle = new String[] {
    "not pre-analyzed",
    "{\"v\":\"1\",\"str\":\"string value\",\"tokens\":[{\"t\":\"foo\"},{\"t\":\"bar\"}]}",
  };
  String[] simpleTeststop = new String[] {
      "1 =this is a test.=one two three",
      "1 =this is a test.=three four five"
  };
  String[] jsonTeststop = new String[] {
      "{\"v\":\"1\",\"str\":\"this is a test.\",\"tokens\":[{\"t\":\"one\"},{\"t\":\"two\"},{\"t\":\"three\"}]}",
      "{\"v\":\"1\",\"str\":\"this is a test.\",\"tokens\":[{\"t\":\"three\"},{\"t\":\"four\"},{\"t\":\"five\"}]}",
  };
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }

  @Test
  public void testSimple() throws Exception {
    test("pre-analyzed-simple", simpleTitle, simpleTeststop);
  }
  
  @Test
  public void testJson() throws Exception {
    test("pre-analyzed-json", jsonTitle, jsonTeststop);
  }

  private void test(String chain, String[] title, String[] teststop) throws Exception {
    SolrInputDocument doc = processAdd(chain,
        doc(f("id", "1"),
            f("title", title[0]),
            f("teststop", teststop[0]),
            f("nonexistent", "foobar"),
            f("ssto", teststop[0]),
            f("sind", teststop[0])));
    assertEquals("title should be unchanged", title[0], doc.getFieldValue("title"));
    assertTrue("teststop should be a Field", doc.getFieldValue("teststop") instanceof Field);
    Field f = (Field)doc.getFieldValue("teststop");
    assertEquals("teststop should have stringValue", "this is a test.", f.stringValue());
    assertNotNull("teststop should have tokensStreamValue", f.tokenStreamValue());
    assertNull("nonexistent should be dropped", doc.getField("nonexistent"));
    // check how SchemaField type affects stored/indexed part processing
    f = (Field)doc.getFieldValue("ssto");
    assertNotNull("should have ssto", f);
    assertNotNull("should have stringValue", f.stringValue());
    assertNull("should not have tokenStreamValue", f.tokenStreamValue());
    f = (Field)doc.getFieldValue("sind");
    assertNotNull("should have sind", f);
    assertNull("should not have stringValue: '" + f.stringValue() + "'", f.stringValue());
    assertNotNull("should have tokenStreamValue", f.tokenStreamValue());
    
    doc = processAdd(chain,
        doc(f("id", "2"),
            f("title", title[1]),
            f("teststop", teststop[1]),
            f("nonexistent", "foobar"),
            f("ssto", teststop[1]),
            f("sind", teststop[1])));
    assertTrue("title should be a Field", doc.getFieldValue("title") instanceof Field);
    assertTrue("teststop should be a Field", doc.getFieldValue("teststop") instanceof Field);
    f = (Field)doc.getFieldValue("teststop");
    assertEquals("teststop should have stringValue", "this is a test.", f.stringValue());
    assertNotNull("teststop should have tokensStreamValue", f.tokenStreamValue());
    assertNull("nonexistent should be dropped", doc.getField("nonexistent"));
    // check how SchemaField type affects stored/indexed part processing
    f = (Field)doc.getFieldValue("ssto");
    assertNotNull("should have ssto", f);
    assertNotNull("should have stringValue", f.stringValue());
    assertNull("should not have tokenStreamValue", f.tokenStreamValue());
    f = (Field)doc.getFieldValue("sind");
    assertNotNull("should have sind", f);
    assertNull("should not have stringValue: '" + f.stringValue() + "'", f.stringValue());
    assertNotNull("should have tokenStreamValue", f.tokenStreamValue());
    
    assertU(commit());
    assertQ(req("teststop:\"one two three\"")
        ,"//str[@name='id'][.='1']"
        ,"//str[@name='teststop'][.='this is a test.']"
        );
    assertQ(req("teststop:three")
        ,"//*[@numFound='2']"
        ,"//result/doc[1]/str[@name='id'][.='1']"
        ,"//result/doc[1]/str[@name='title'][.='not pre-analyzed']"
        ,"//result/doc[2]/str[@name='id'][.='2']"
        ,"//result/doc[2]/arr[@name='title']/str[.='string value']"
        );
    assertQ(req("ssto:three"), "//*[@numFound='0']");
    assertQ(req("sind:three"), "//*[@numFound='2']");
  }  
}
