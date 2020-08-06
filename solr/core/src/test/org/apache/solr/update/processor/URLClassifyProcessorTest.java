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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.BeforeClass;
import org.junit.Test;

public class URLClassifyProcessorTest extends SolrTestCaseJ4 {
  
  private static URLClassifyProcessor classifyProcessor;
  
  @BeforeClass
  public static void initTest() {
    classifyProcessor =
      (URLClassifyProcessor) new URLClassifyProcessorFactory().getInstance(null, null, null);
  }
  
  @Test
  public void testProcessor() throws IOException {
    AddUpdateCommand addCommand = new AddUpdateCommand(null);
    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", "test");
    document.addField("url", "http://www.example.com");
    addCommand.solrDoc = document;
    classifyProcessor.processAdd(addCommand);
    assertEquals("Confirm single valued field returned",1, document.getField("url_length").getValueCount());
    assertEquals("Confirm field populated",22, document.getField("url_length").getValue());
  }
  
  @Test
  public void testNormalizations() throws MalformedURLException, URISyntaxException {
    String url1 = "http://www.example.com/research/";
    String url2 = "http://www.example.com/research/../research/";
    assertEquals(classifyProcessor.getNormalizedURL(url1), classifyProcessor.getNormalizedURL(url2));
  }
  
  @Test
  public void testLength() throws MalformedURLException, URISyntaxException {
    assertEquals(22, classifyProcessor.length(classifyProcessor.getNormalizedURL("http://www.example.com")));
  }
  
  @Test
  public void testLevels() throws MalformedURLException, URISyntaxException {
    assertEquals(1, classifyProcessor.levels(classifyProcessor.getNormalizedURL("http://www.example.com/research/")));
    assertEquals(1, classifyProcessor.levels(classifyProcessor.getNormalizedURL("http://www.example.com/research/index.html")));
    assertEquals(1, classifyProcessor.levels(classifyProcessor.getNormalizedURL("http://www.example.com/research/../research/")));
    assertEquals(0, classifyProcessor.levels(classifyProcessor.getNormalizedURL("http://www.example.com/")));
    assertEquals(0, classifyProcessor.levels(classifyProcessor.getNormalizedURL("http://www.example.com/index.htm")));
    assertEquals(0, classifyProcessor.levels(classifyProcessor.getNormalizedURL("http://www.example.com")));
    assertEquals(0, classifyProcessor.levels(classifyProcessor.getNormalizedURL("https://www.example.com")));
    assertEquals(0, classifyProcessor.levels(classifyProcessor.getNormalizedURL("http://www.example.com////")));
  }
  
  @Test
  public void testLandingPage() throws MalformedURLException, URISyntaxException {
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/index.html")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/index.htm")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/welcome.html")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/welcome.htm")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/index.php")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/index.asp")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/research/")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("https://www.example.com/research/")));
    assertTrue(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/")));
    assertFalse(classifyProcessor.isLandingPage(classifyProcessor.getNormalizedURL("http://www.example.com/intro.htm")));
  }
  
  @Test
  public void testTopLevelPage() throws MalformedURLException, URISyntaxException {
    assertTrue(classifyProcessor.isTopLevelPage(classifyProcessor.getNormalizedURL("http://www.example.com")));
    assertTrue(classifyProcessor.isTopLevelPage(classifyProcessor.getNormalizedURL("http://www.example.com/")));
    assertTrue(classifyProcessor.isTopLevelPage(classifyProcessor.getNormalizedURL("http://subdomain.example.com:1234/#anchor")));
    assertTrue(classifyProcessor.isTopLevelPage(classifyProcessor.getNormalizedURL("http://www.example.com/index.html")));
    
    assertFalse(classifyProcessor.isTopLevelPage(classifyProcessor.getNormalizedURL("http://www.example.com/foo")));
    assertFalse(classifyProcessor.isTopLevelPage(classifyProcessor.getNormalizedURL("http://subdomain.example.com/?sorting=lastModified%253Adesc&tag=myTag&view=feed")));
  }
  
  @Test
  public void testCanonicalUrl() throws MalformedURLException, URISyntaxException {
    assertEquals("http://www.example.com/", classifyProcessor.getCanonicalUrl(classifyProcessor.getNormalizedURL("http://www.example.com/index.html")).toString());
  }
}
