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

package org.apache.solr.client.solrj;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class SolrQueryTest extends TestCase {
  
  public void testSolrQueryMethods() {
    SolrQuery q = new SolrQuery("dog");
    boolean b = false;
    
    q.setFacetLimit(10);
    q.addFacetField("price");
    q.addFacetField("state");
    Assert.assertEquals(q.getFacetFields().length, 2);
    q.addFacetQuery("instock:true");
    q.addFacetQuery("instock:false");
    q.addFacetQuery("a:b");
    Assert.assertEquals(q.getFacetQuery().length, 3);
    
    b = q.removeFacetField("price");
    Assert.assertEquals(b, true);
    b = q.removeFacetField("price2");
    Assert.assertEquals(b, false);
    b = q.removeFacetField("state");
    Assert.assertEquals(b, true);
    Assert.assertEquals(null, q.getFacetFields());
    
    b = q.removeFacetQuery("instock:true");
    Assert.assertEquals(b, true);
    b = q.removeFacetQuery("instock:false");
    b = q.removeFacetQuery("a:c");
    Assert.assertEquals(b, false);
    b = q.removeFacetQuery("a:b");
    Assert.assertEquals(null, q.getFacetQuery());   
    
    q.addSortField("price", SolrQuery.ORDER.asc);
    q.addSortField("date", SolrQuery.ORDER.desc);
    q.addSortField("qty", SolrQuery.ORDER.desc);
    q.removeSortField("date", SolrQuery.ORDER.desc);
    Assert.assertEquals(2, q.getSortFields().length);
    q.removeSortField("price", SolrQuery.ORDER.asc);
    q.removeSortField("qty", SolrQuery.ORDER.desc);
    Assert.assertEquals(null, q.getSortFields());
    
    q.addHighlightField("hl1");
    q.addHighlightField("hl2");
    q.setHighlightSnippets(2);
    Assert.assertEquals(2, q.getHighlightFields().length);
    Assert.assertEquals(100, q.getHighlightFragsize());
    Assert.assertEquals(q.getHighlightSnippets(), 2);
    q.removeHighlightField("hl1");
    q.removeHighlightField("hl3");
    Assert.assertEquals(1, q.getHighlightFields().length);
    q.removeHighlightField("hl2");
    Assert.assertEquals(null, q.getHighlightFields());
    
    // check to see that the removes are properly clearing the cgi params
    Assert.assertEquals(q.toString(), "q=dog");

    //Add time allowed param
    q.setTimeAllowed(1000);
    Assert.assertEquals((Integer)1000, q.getTimeAllowed() );
    //Adding a null should remove it
    q.setTimeAllowed(null);
    Assert.assertEquals(null, q.getTimeAllowed() ); 
    
    System.out.println(q);
  }
  
  public void testFacetSort() {
    SolrQuery q = new SolrQuery("dog");
    assertTrue("expected default value to be true", q.getFacetSort());
    q.setFacetSort(false);
    assertFalse("expected set value to be false", q.getFacetSort());
  }
}
