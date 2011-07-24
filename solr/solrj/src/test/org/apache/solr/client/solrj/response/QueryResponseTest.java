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

import junit.framework.Assert;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

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

  @Test
  public void testRangeFacets() throws Exception {
    XMLResponseParser parser = new XMLResponseParser();
    InputStream is = new SolrResourceLoader(null, null).openResource("solrj/sampleDateFacetResponse.xml");
    assertNotNull(is);
    Reader in = new InputStreamReader(is, "UTF-8");
    NamedList<Object> response = parser.processResponse(in);
    in.close();

    QueryResponse qr = new QueryResponse(response, null);
    Assert.assertNotNull(qr);

    int counter = 0;
    RangeFacet.Numeric price = null;
    RangeFacet.Date manufacturedateDt = null;
    for (RangeFacet r : qr.getFacetRanges()){
      assertNotNull(r);
      if ("price".equals(r.getName())) {
        price = (RangeFacet.Numeric) r;
      } else if ("manufacturedate_dt".equals(r.getName())) {
        manufacturedateDt = (RangeFacet.Date) r;
      }

      counter++;
    }
    assertEquals(2, counter);
    assertNotNull(price);
    assertNotNull(manufacturedateDt);

    assertEquals(0.0F, price.getStart());
    assertEquals(5.0F, price.getEnd());
    assertEquals(1.0F, price.getGap());
    assertEquals("0.0", price.getCounts().get(0).getValue());
    assertEquals(3, price.getCounts().get(0).getCount());
    assertEquals("1.0", price.getCounts().get(1).getValue());
    assertEquals(0, price.getCounts().get(1).getCount());
    assertEquals("2.0", price.getCounts().get(2).getValue());
    assertEquals(0, price.getCounts().get(2).getCount());
    assertEquals("3.0", price.getCounts().get(3).getValue());
    assertEquals(0, price.getCounts().get(3).getCount());
    assertEquals("4.0", price.getCounts().get(4).getValue());
    assertEquals(0, price.getCounts().get(4).getCount());

    assertEquals(DateUtil.parseDate("2005-02-13T15:26:37Z"), manufacturedateDt.getStart());
    assertEquals(DateUtil.parseDate("2008-02-13T15:26:37Z"), manufacturedateDt.getEnd());
    assertEquals("+1YEAR", manufacturedateDt.getGap());
    assertEquals("2005-02-13T15:26:37Z", manufacturedateDt.getCounts().get(0).getValue());
    assertEquals(4, manufacturedateDt.getCounts().get(0).getCount());
    assertEquals("2006-02-13T15:26:37Z", manufacturedateDt.getCounts().get(1).getValue());
    assertEquals(7, manufacturedateDt.getCounts().get(1).getCount());
    assertEquals("2007-02-13T15:26:37Z", manufacturedateDt.getCounts().get(2).getValue());
    assertEquals(0, manufacturedateDt.getCounts().get(2).getCount());
  }

}
