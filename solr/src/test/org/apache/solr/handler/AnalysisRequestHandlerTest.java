package org.apache.solr.handler;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.AbstractSolrTestCase;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.StringReader;

public class AnalysisRequestHandlerTest extends AbstractSolrTestCase {
  private XMLInputFactory inputFactory = XMLInputFactory.newInstance();

  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }


  public void testReadDoc() throws Exception {
    String xml =
            "<docs><doc >" +
                    "  <field name=\"id\" >12345</field>" +
                    "  <field name=\"name\">cute little kitten</field>" +
                    "  <field name=\"text\">the quick red fox jumped over the lazy brown dogs</field>" +
                    "</doc>" +
                    "<doc >" +
                    "  <field name=\"id\" >12346</field>" +
                    "  <field name=\"name\">big mean dog</field>" +
                    "  <field name=\"text\">cats like to purr</field>" +
                    "</doc>" +
                    "</docs>";

    XMLStreamReader parser =
            inputFactory.createXMLStreamReader(new StringReader(xml));
    AnalysisRequestHandler handler = new AnalysisRequestHandler();
    NamedList<Object> result = handler.processContent(parser, h.getCore().getSchema());
    assertTrue("result is null and it shouldn't be", result != null);
    NamedList<NamedList<NamedList<Object>>> theTokens = (NamedList<NamedList<NamedList<Object>>>) result.get("12345");
    assertTrue("theTokens is null and it shouldn't be", theTokens != null);
    NamedList<NamedList<Object>> tokens = theTokens.get("name");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertTrue("tokens Size: " + tokens.size() + " is not : " + 3, tokens.size() == 3);
    NamedList<Object> token;
    String value;
    token = tokens.get("token", 0);
    value = (String) token.get("value");
    assertTrue(value + " is not equal to " + "cute", value.equals("cute") == true);
    token = tokens.get("token", 1);
    value = (String) token.get("value");
    assertTrue(value + " is not equal to " + "little", value.equals("little") == true);

    token = tokens.get("token", 2);
    value = (String) token.get("value");
    assertTrue(value + " is not equal to " + "kitten", value.equals("kitten") == true);

    tokens = theTokens.get("text");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertTrue("tokens Size: " + tokens.size() + " is not : " + 8, tokens.size() == 8);//stopwords are removed

    String[] gold = new String[]{"quick", "red", "fox", "jump", "over", "lazi", "brown", "dog"};
    for (int j = 0; j < gold.length; j++) {
      NamedList<Object> tok = tokens.get("token", j);
      value = (String) tok.get("value");
      assertTrue(value + " is not equal to " + gold[j], value.equals(gold[j]) == true);
    }
    theTokens = (NamedList<NamedList<NamedList<Object>>>) result.get("12346");
    assertTrue("theTokens is null and it shouldn't be", theTokens != null);
    tokens = theTokens.get("name");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertTrue("tokens Size: " + tokens.size() + " is not : " + 3, tokens.size() == 3);
    gold = new String[]{"cat", "like", "purr"};
    tokens = theTokens.get("text");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertTrue("tokens Size: " + tokens.size() + " is not : " + 3, tokens.size() == 3);//stopwords are removed
    for (int j = 0; j < gold.length; j++) {
      NamedList<Object> tok = tokens.get("token", j);
      value = (String) tok.get("value");
      assertTrue(value + " is not equal to " + gold[j], value.equals(gold[j]) == true);
    }
  }
}