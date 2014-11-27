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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.core.JmxMonitoredMap.SolrDynamicMBean;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for JmxMonitoredMap
 *
 *
 * @since solr 1.3
 */
public class TestSolrDynamicMBean extends LuceneTestCase {


  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }


  @Test
  public void testCachedStatsOption() throws Exception{
    //  SOLR-6747 Add an optional caching option as a workaround for SOLR-6586.
    
    SolrInfoMBean solrmbeaninfo = new MockInfoMBean();
    SolrDynamicMBean sdmbean = new SolrDynamicMBean("", solrmbeaninfo);
    
    sdmbean.getMBeanInfo();
    
    Object object1 = sdmbean.getAttribute("Object");
    Object object2 = sdmbean.getAttribute("Object");
    
    assertNotSame(object1, object2);
    
    sdmbean.getMBeanInfo();
    
    Object object12 = sdmbean.getAttribute("Object");
    Object object22 = sdmbean.getAttribute("Object");
    
    assertNotSame(object1, object12);
    assertNotSame(object2, object22);
    
    
    // test cached stats
    
    solrmbeaninfo = new MockInfoMBean();
    sdmbean = new SolrDynamicMBean("", solrmbeaninfo, true);
    
    sdmbean.getMBeanInfo();
    
    object1 = sdmbean.getAttribute("Object");
    object2 = sdmbean.getAttribute("Object");
    
    assertEquals(object1, object2);
    
    sdmbean.getMBeanInfo();
    
    object12 = sdmbean.getAttribute("Object");
    object22 = sdmbean.getAttribute("Object");
    
    assertNotSame(object1, object12);
    assertNotSame(object2, object22);
    
    assertEquals(object12, object22);
    
  }

}
