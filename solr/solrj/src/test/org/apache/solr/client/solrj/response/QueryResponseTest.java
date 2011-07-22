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

package org.apache.solr.client.solrj.response;

import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import junit.framework.Assert;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

/**
 * Simple test for Date facet support in QueryResponse
 * 
 * @since solr 1.3
 */
public class QueryResponseTest extends LuceneTestCase {
  @Test
  public void testDateFacets() throws Exception   {
    XMLResponseParser parser = new XMLResponseParser();
    InputStream is = new SolrResourceLoader(null, null).openResource("solrj/sampleDateFacetResponse.xml");
    assertNotNull(is);
    Reader in = new InputStreamReader(is, "UTF-8");
    NamedList<Object> response = parser.processResponse(in);
    in.close();
    
    QueryResponse qr = new QueryResponse(response, null);
    Assert.assertNotNull(qr);
    
    Assert.assertNotNull(qr.getFacetDates());
    
    for (FacetField f : qr.getFacetDates()) {
      Assert.assertNotNull(f);

      // TODO - test values?
      // System.out.println(f.toString());
      // System.out.println("GAP: " + f.getGap());
      // System.out.println("END: " + f.getEnd());
    }
  }
}
