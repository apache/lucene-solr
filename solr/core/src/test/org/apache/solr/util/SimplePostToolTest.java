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
package org.apache.solr.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.SimplePostTool.PageFetcher;
import org.apache.solr.util.SimplePostTool.PageFetcherResult;
import org.junit.Before;
import org.junit.Test;

/**
 * NOTE: do *not* use real hostnames, not even "example.com", in this test.
 *
 * A MockPageFetcher is used to prevent real HTTP requests from being executed.
 */ 
public class SimplePostToolTest extends SolrTestCaseJ4 {

  SimplePostTool t_file, t_file_auto, t_file_rec, t_web, t_test;
  PageFetcher pf;
  
  @Before
  public void initVariousPostTools() throws Exception {
    String[] args = {"-"};
    
    // Add a dummy core/collection property so that the SimplePostTool
    // doesn't fail fast. 
    System.setProperty("c", "testcollection");
    
    System.setProperty("data", "files");
    t_file = SimplePostTool.parseArgsAndInit(args);

    System.setProperty("auto", "yes");
    t_file_auto = SimplePostTool.parseArgsAndInit(args);

    System.setProperty("recursive", "yes");
    t_file_rec = SimplePostTool.parseArgsAndInit(args);
    
    System.setProperty("data", "web");
    t_web = SimplePostTool.parseArgsAndInit(args);

    System.setProperty("params", "param1=foo&param2=bar");
    System.setProperty("url", "http://user:password@localhost:5150/solr/update");
    t_test = SimplePostTool.parseArgsAndInit(args);

    pf = new MockPageFetcher();
    for (SimplePostTool mockable : new SimplePostTool[]{t_web, t_file_auto}) {
      mockable.pageFetcher = pf;
      mockable.mockMode = true;
    }
  }
  
  @Test
  public void testParseArgsAndInit() {
    assertEquals(false, t_file.auto);
    assertEquals(true, t_file_auto.auto);
    assertEquals(0, t_file_auto.recursive);
    assertEquals(999, t_file_rec.recursive);
    assertEquals(true, t_file.commit);
    assertEquals(false, t_file.optimize);
    assertEquals(null, t_file.out);

    assertEquals(1, t_web.recursive);
    assertEquals(10, t_web.delay);
    
    assertEquals("http://user:password@localhost:5150/solr/update?param1=foo&param2=bar",t_test.solrUrl.toExternalForm());
  }
  
  @Test
  public void testNormalizeUrlEnding() {
    assertEquals("http://[ff01::114]", SimplePostTool.normalizeUrlEnding("http://[ff01::114]/"));
    assertEquals("http://[ff01::114]", SimplePostTool.normalizeUrlEnding("http://[ff01::114]/#foo?bar=baz"));
    assertEquals("http://[ff01::114]/index.html", SimplePostTool.normalizeUrlEnding("http://[ff01::114]/index.html#hello"));
  }
  
  @Test
  public void testComputeFullUrl() throws MalformedURLException {
    assertEquals("http://[ff01::114]/index.html", t_web.computeFullUrl(new URL("http://[ff01::114]/"), "/index.html"));
    assertEquals("http://[ff01::114]/index.html", t_web.computeFullUrl(new URL("http://[ff01::114]/foo/bar/"), "/index.html"));
    assertEquals("http://[ff01::114]/fil.html", t_web.computeFullUrl(new URL("http://[ff01::114]/foo.htm?baz#hello"), "fil.html"));
//    TODO: How to know what is the base if URL path ends with "foo"?? 
//    assertEquals("http://[ff01::114]/fil.html", t_web.computeFullUrl(new URL("http://[ff01::114]/foo?baz#hello"), "fil.html"));
    assertEquals(null, t_web.computeFullUrl(new URL("http://[ff01::114]/"), "fil.jpg"));
    assertEquals(null, t_web.computeFullUrl(new URL("http://[ff01::114]/"), "mailto:hello@foo.bar"));
    assertEquals(null, t_web.computeFullUrl(new URL("http://[ff01::114]/"), "ftp://server/file"));
  }
  
  @Test
  public void testTypeSupported() {
    assertTrue(t_web.typeSupported("application/pdf"));
    assertTrue(t_web.typeSupported("application/xml"));
    assertFalse(t_web.typeSupported("text/foo"));

    t_web.fileTypes = "doc,xls,ppt";
    t_web.fileFilter = t_web.getFileFilterFromFileTypes(t_web.fileTypes);
    assertFalse(t_web.typeSupported("application/pdf"));
    assertTrue(t_web.typeSupported("application/msword"));
  }
  
  @Test
  public void testIsOn() {
    assertTrue(SimplePostTool.isOn("true"));
    assertTrue(SimplePostTool.isOn("1"));
    assertFalse(SimplePostTool.isOn("off"));
  }
  
  @Test
  public void testAppendParam() {
    assertEquals("http://[ff01::114]?foo=bar", SimplePostTool.appendParam("http://[ff01::114]", "foo=bar"));
    assertEquals("http://[ff01::114]/?a=b&foo=bar", SimplePostTool.appendParam("http://[ff01::114]/?a=b", "foo=bar"));
  }
  
  @Test
  public void testAppendUrlPath() throws MalformedURLException {
    assertEquals(new URL("http://[ff01::114]/a?foo=bar"), SimplePostTool.appendUrlPath(new URL("http://[ff01::114]?foo=bar"), "/a"));
  }
  
  @Test
  public void testGuessType() {
    File f = new File("foo.doc");
    assertEquals("application/msword", SimplePostTool.guessType(f));
    f = new File("foobar");
    assertEquals("application/octet-stream", SimplePostTool.guessType(f));
    f = new File("foo.jsonl");
    assertEquals("application/json", SimplePostTool.guessType(f));
  }

  @Test
  public void testDoFilesMode() {
    t_file_auto.recursive = 0;
    File dir = getFile("exampledocs");
    int num = t_file_auto.postFiles(new File[] {dir}, 0, null, null);
    assertEquals(2, num);
  }

  @Test
  public void testDoWebMode() {
    // Uses mock pageFetcher
    t_web.delay = 0;
    t_web.recursive = 5;
    int num = t_web.postWebPages(new String[] {"http://[ff01::114]/#removeme"}, 0, null);
    assertEquals(5, num);
    
    t_web.recursive = 1;
    num = t_web.postWebPages(new String[] {"http://[ff01::114]/"}, 0, null);
    assertEquals(3, num);
    
    // Without respecting robots.txt
    t_web.pageFetcher.robotsCache.put("[ff01::114]", Collections.emptyList());
    t_web.recursive = 5;
    num = t_web.postWebPages(new String[] {"http://[ff01::114]/#removeme"}, 0, null);
    assertEquals(6, num);
}
  
  @Test
  public void testRobotsExclusion() throws MalformedURLException {
    assertFalse(t_web.pageFetcher.isDisallowedByRobots(new URL("http://[ff01::114]/")));
    assertTrue(t_web.pageFetcher.isDisallowedByRobots(new URL("http://[ff01::114]/disallowed")));
    assertTrue("There should be two entries parsed from robots.txt", t_web.pageFetcher.robotsCache.get("[ff01::114]").size() == 2);
  }

  static class MockPageFetcher extends PageFetcher {
    HashMap<String,String> htmlMap = new HashMap<>();
    HashMap<String,Set<URL>> linkMap = new HashMap<>();
    
    public MockPageFetcher() throws IOException {
      (new SimplePostTool()).super();
      htmlMap.put("http://[ff01::114]", "<html><body><a href=\"http://[ff01::114]/page1\">page1</a><a href=\"http://[ff01::114]/page2\">page2</a></body></html>");
      htmlMap.put("http://[ff01::114]/index.html", "<html><body><a href=\"http://[ff01::114]/page1\">page1</a><a href=\"http://[ff01::114]/page2\">page2</a></body></html>");
      htmlMap.put("http://[ff01::114]/page1", "<html><body><a href=\"http://[ff01::114]/page1/foo\"></body></html>");
      htmlMap.put("http://[ff01::114]/page1/foo", "<html><body><a href=\"http://[ff01::114]/page1/foo/bar\"></body></html>");
      htmlMap.put("http://[ff01::114]/page1/foo/bar", "<html><body><a href=\"http://[ff01::114]/page1\"></body></html>");
      htmlMap.put("http://[ff01::114]/page2", "<html><body><a href=\"http://[ff01::114]/\"><a href=\"http://[ff01::114]/disallowed\"/></body></html>");
      htmlMap.put("http://[ff01::114]/disallowed", "<html><body><a href=\"http://[ff01::114]/\"></body></html>");

      Set<URL> s = new HashSet<>();
      s.add(new URL("http://[ff01::114]/page1"));
      s.add(new URL("http://[ff01::114]/page2"));
      linkMap.put("http://[ff01::114]", s);
      linkMap.put("http://[ff01::114]/index.html", s);
      s = new HashSet<>();
      s.add(new URL("http://[ff01::114]/page1/foo"));
      linkMap.put("http://[ff01::114]/page1", s);
      s = new HashSet<>();
      s.add(new URL("http://[ff01::114]/page1/foo/bar"));
      linkMap.put("http://[ff01::114]/page1/foo", s);
      s = new HashSet<>();
      s.add(new URL("http://[ff01::114]/disallowed"));
      linkMap.put("http://[ff01::114]/page2", s);
      
      // Simulate a robots.txt file with comments and a few disallows
      StringBuilder sb = new StringBuilder();
      sb.append("# Comments appear after the \"#\" symbol at the start of a line, or after a directive\n");
      sb.append("User-agent: * # match all bots\n");
      sb.append("Disallow:  # This is void\n");
      sb.append("Disallow: /disallow # Disallow this path\n");
      sb.append("Disallow: /nonexistingpath # Disallow this path\n");
      this.robotsCache.put("[ff01::114]", super.
          parseRobotsTxt(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8))));
    }
    
    @Override
    public PageFetcherResult readPageFromUrl(URL u) {
      PageFetcherResult res = new PageFetcherResult();
      if (isDisallowedByRobots(u)) {
        res.httpStatus = 403;
        return res;
      }
      res.httpStatus = 200;
      res.contentType = "text/html";
      res.content = ByteBuffer.wrap( htmlMap.get(u.toString()).getBytes(StandardCharsets.UTF_8));
      return res;
    }
    
    @Override
    public Set<URL> getLinksFromWebPage(URL u, InputStream is, String type, URL postUrl) {
      Set<URL> s = linkMap.get(SimplePostTool.normalizeUrlEnding(u.toString()));
      if(s == null)
        s = new HashSet<>();
      return s;
    }
  }
}
