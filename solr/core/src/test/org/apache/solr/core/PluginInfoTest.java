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
package org.apache.solr.core;

import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.DOMUtilTestBase;
import org.junit.Test;
import org.w3c.dom.Node;

/**
 * TestCase  for PluginInfo.
 * Extends DOMUtilTestBase because PluginInfo heavily depends on DOMUtil 
 * and the convinient {@link #getNode(String, String)} method.
 */
public class PluginInfoTest extends DOMUtilTestBase {

  private final static String configWithNoChildren = "" +
    "<plugin name=\"aplug\">" +
    "<str name=\"stringer\">aString</str>" +
    "<int name=\"inter\">1</int>" +
    "<bool name=\"booler\">true</bool>" +
    "<float name=\"floater\">1.1f</float>" +
    "<double name=\"doubler\">2.2d</double>" +
    "<long name=\"longer\">2</long>" +
    "<lst name=\"lister\">" +
    "</lst>" +
    "<arr name=\"arrer\">" +
    "<str name=\"stringer\">aString</str>" +
    "</arr>" +
    "</plugin>";
  private final static String configWith2Children = "" +
    "<plugin name=\"aplug\">" +
    "<int name=\"inter\">1</int>" +
    "<child name=\"child1\"><int name=\"index\">0</int></child>" +
    "<child name=\"child2\"><int name=\"index\">1</int></child>" +
    "<float name=\"floater\">1.1f</float>" +
    "<double name=\"doubler\">2.2d</double>" +
    "<long name=\"longer\">2</long>" +
    "<lst name=\"lister\">" +
    "</lst>" +
    "<arr name=\"arrer\">" +
    "<str name=\"stringer\">aString</str>" +
    "</arr>" +
    "</plugin>";

  // This is in fact a DOMUtil test, but it is here for completeness  
  @Test
  public void testNameRequired() throws Exception {
    Node nodeWithNoName = getNode("<plugin></plugin>", "plugin");
    try {
      SolrTestCaseJ4.ignoreException("missing mandatory attribute");
      RuntimeException thrown = expectThrows(RuntimeException.class, () -> {
        PluginInfo pi = new PluginInfo(nodeWithNoName, "Node with No name", true, false);
      });
      assertTrue(thrown.getMessage().contains("missing mandatory attribute"));
    } finally {
      SolrTestCaseJ4.resetExceptionIgnores();
    }

    Node nodeWithAName = getNode("<plugin name=\"myName\" />", "plugin");
    PluginInfo pi2 = new PluginInfo(nodeWithAName, "Node with a Name", true, false);
    assertTrue(pi2.name.equals("myName"));
  }
  
  @Test
  public void testClassRequired() throws Exception {
    Node nodeWithNoClass = getNode("<plugin></plugin>", "plugin");
    try {
      SolrTestCaseJ4.ignoreException("missing mandatory attribute");
      RuntimeException thrown = expectThrows(RuntimeException.class, () -> {
        PluginInfo pi = new PluginInfo(nodeWithNoClass, "Node with No Class", false, true);
      });
      assertTrue(thrown.getMessage().contains("missing mandatory attribute"));
    } finally {
      SolrTestCaseJ4.resetExceptionIgnores();
    }

    Node nodeWithAClass = getNode("<plugin class=\"myName\" />", "plugin");
    PluginInfo pi2 = new PluginInfo(nodeWithAClass, "Node with a Class", false, true);
    assertTrue(pi2.className.equals("myName"));
  }

  @Test
  public void testIsEnabled() throws Exception {
    Node node = getNode("<plugin enable=\"true\" />", "plugin");
    PluginInfo pi = new PluginInfo(node, "enabled", false, false);
    assertTrue(pi.isEnabled());
    node = getNode("<plugin enable=\"false\" />", "plugin");
    pi = new PluginInfo(node, "not enabled", false, false);
    assertFalse(pi.isEnabled());
    
  }

  @Test
  public void testIsDefault() throws Exception {
    Node node = getNode("<plugin default=\"true\" />", "plugin");
    PluginInfo pi = new PluginInfo(node, "default", false, false);
    assertTrue(pi.isDefault());
    node = getNode("<plugin default=\"false\" />", "plugin");
    pi = new PluginInfo(node, "not default", false, false);
    assertFalse(pi.isDefault());
    
  }

  @Test
  public void testNoChildren() throws Exception{
    Node node = getNode(configWithNoChildren, "/plugin");
    PluginInfo pi = new PluginInfo(node, "from static", false, false);
    assertTrue(pi.children.isEmpty());
  }

  @Test
  public void testHasChildren() throws Exception {
    Node node = getNode(configWith2Children, "plugin");
    PluginInfo pi = new PluginInfo(node, "node with 2 Children", false, false);
    assertTrue( pi.children.size() == 2 );
  }

  @Test
  public void testChild() throws Exception {
    Node node = getNode(configWith2Children, "plugin");
    PluginInfo pi = new PluginInfo(node, "with children", false, false);
    PluginInfo childInfo = pi.getChild("child");
    assertNotNull(childInfo);
    PluginInfo notExistent = pi.getChild("doesnotExist");
    assertNull(notExistent);
    assertTrue( childInfo instanceof PluginInfo );
    assertTrue((Integer) childInfo.initArgs.get("index") == 0);
    Node node2 = getNode(configWithNoChildren, "plugin");
    PluginInfo pi2 = new PluginInfo(node2, "with No Children", false, false);
    PluginInfo noChild = pi2.getChild("long");
    assertNull(noChild);
  }

  @Test
  public void testChildren() throws Exception {
    Node node = getNode(configWith2Children, "plugin");
    PluginInfo pi = new PluginInfo(node, "with children", false, false);
    List<PluginInfo> children = pi.getChildren("child");
    assertTrue(children.size() == 2);
    for ( PluginInfo childInfo : children ) {
      assertNotNull(childInfo);
      assertTrue( childInfo instanceof PluginInfo );
    }
  }

  @Test
  public void testInitArgsCount() throws Exception {
    Node node = getNode(configWithNoChildren, "plugin");
    PluginInfo pi = new PluginInfo(node, "from static", true, false);
    assertTrue( pi.initArgs.size() == node.getChildNodes().getLength() );
  }
}
