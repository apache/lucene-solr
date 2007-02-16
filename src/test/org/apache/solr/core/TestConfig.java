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

package org.apache.solr.core;

import org.apache.solr.util.AbstractSolrTestCase;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;

public class TestConfig extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  public void testJavaProperty() {
    // property values defined in build.xml

    String s = SolrConfig.config.get("propTest");
    assertEquals("prefix-proptwo-suffix", s);

    s = SolrConfig.config.get("propTest/@attr1", "default");
    assertEquals("propone-${literal}", s);

    s = SolrConfig.config.get("propTest/@attr2", "default");
    assertEquals("default-from-config", s);

    s = SolrConfig.config.get("propTest[@attr2='default-from-config']", "default");
    assertEquals("prefix-proptwo-suffix", s);

    NodeList nl = (NodeList) SolrConfig.config.evaluate("propTest", XPathConstants.NODESET);
    assertEquals(1, nl.getLength());
    assertEquals("prefix-proptwo-suffix", nl.item(0).getTextContent());

    Node node = SolrConfig.config.getNode("propTest", true);
    assertEquals("prefix-proptwo-suffix", node.getTextContent());
  }
}