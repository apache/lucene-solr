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
package org.apache.solr.client.solrj.impl;


import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.RandomizeSSL;
import org.junit.Before;
@RandomizeSSL(1.0)
@SolrTestCase.AlwaysUseSSL
@LuceneTestCase.Nightly
public class HttpSolrClientSSLAuthConPoolTest extends HttpSolrClientConPoolTest {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    String[] urls = new String[] {jetty.getBaseUrl(), yetty.getBaseUrl()};
    for (String u : urls) {
      assertTrue("expect https urls ", u.startsWith("https"));
    }
    assertFalse("expect different urls " + Arrays.toString(urls), urls[0].equals(urls[1]));
  }
}
