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


import java.net.URL;
import java.util.Arrays;

import org.apache.solr.util.RandomizeSSL;
import org.junit.BeforeClass;

@RandomizeSSL(1.0)
public class HttpSolrClientSSLAuthConPoolTest extends HttpSolrClientConPoolTest {

    @BeforeClass
    public static void checkUrls() throws Exception {
      URL[] urls = new URL[] {
          jetty.getBaseUrl(), yetty.getBaseUrl() 
      };
      for (URL u : urls) {
        assertEquals("expect https urls ","https", u.getProtocol());
      }
      assertFalse("expect different urls "+Arrays.toString(urls),
              urls[0].equals(urls[1]));
    }
}
