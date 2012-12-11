package org.apache.solr.schema;
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests currency field type.
 */
public class OpenExchangeRatesOrgProviderTest extends SolrTestCaseJ4 {
  OpenExchangeRatesOrgProvider oerp;
  ResourceLoader loader;
  private final Map<String,String> emptyParams = new HashMap<String,String>();
  private Map<String,String> mockParams;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    mockParams = new HashMap<String,String>();;
    mockParams.put(OpenExchangeRatesOrgProvider.PARAM_RATES_FILE_LOCATION, "open-exchange-rates.json");  
    oerp = new OpenExchangeRatesOrgProvider();
    loader = new SolrResourceLoader("solr/collection1");
  }
  
  @Test
  public void testInit() throws Exception {
    oerp.init(emptyParams);
    assertTrue("Wrong default url", oerp.ratesFileLocation.toString().equals("http://openexchangerates.org/latest.json"));
    assertTrue("Wrong default interval", oerp.refreshInterval == 1440);

    Map<String,String> params = new HashMap<String,String>();
    params.put(OpenExchangeRatesOrgProvider.PARAM_RATES_FILE_LOCATION, "http://foo.bar/baz");
    params.put(OpenExchangeRatesOrgProvider.PARAM_REFRESH_INTERVAL, "100");
    oerp.init(params);
    assertTrue("Wrong param set url", oerp.ratesFileLocation.equals("http://foo.bar/baz"));
    assertTrue("Wrong param interval", oerp.refreshInterval == 100);
  }

  @Test
  public void testList() {
    oerp.init(mockParams);
    oerp.inform(loader);
    assertEquals(159,     oerp.listAvailableCurrencies().size());
  }

  @Test
  public void testGetExchangeRate() {
    oerp.init(mockParams);
    oerp.inform(loader);
    assertTrue(5.73163 == oerp.getExchangeRate("USD", "NOK"));    
  }

  @Test
  public void testReload() {
    oerp.init(mockParams);
    oerp.inform(loader);
    assertTrue(oerp.reload());
    assertEquals("USD", oerp.rates.getBaseCurrency());
    assertEquals(new Long(1332070464L), new Long(oerp.rates.getTimestamp()));
  }

  @Test(expected=SolrException.class)
  public void testNoInit() {
    oerp.getExchangeRate("ABC", "DEF");
    assertTrue("Should have thrown exception if not initialized", false);
  }
}
