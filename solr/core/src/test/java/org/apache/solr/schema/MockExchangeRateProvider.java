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
package org.apache.solr.schema;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * Simple mock provider with fixed rates and some assertions
 */
public class MockExchangeRateProvider implements ExchangeRateProvider {
  private static Map<String,Double> map = new HashMap<>();
  static {
    map.put("USD,EUR", 0.8);
    map.put("EUR,USD", 1.2);
    map.put("USD,NOK", 5.0);
    map.put("NOK,USD", 0.2);
    map.put("EUR,NOK", 10.0);
    map.put("NOK,EUR", 0.1);
  }

  private boolean gotArgs = false;
  private boolean gotLoader = false;
  
  @Override
  public double getExchangeRate(String sourceCurrencyCode, String targetCurrencyCode) {
//    System.out.println("***** getExchangeRate("+sourceCurrencyCode+targetCurrencyCode+")");
    if(sourceCurrencyCode.equals(targetCurrencyCode)) return 1.0;

    Double result = map.get(sourceCurrencyCode+","+targetCurrencyCode);
    if(result == null) {
      throw new SolrException(ErrorCode.NOT_FOUND, "No exchange rate found for the pair "+sourceCurrencyCode+","+targetCurrencyCode);
    }
    return result;
  }

  @Override
  public Set<String> listAvailableCurrencies() {
    Set<String> currenciesPairs = map.keySet();
    Set<String> returnSet;
    
    returnSet = new HashSet<>();
    for (String c : currenciesPairs) {
      String[] pairs = c.split(",");
      returnSet.add(pairs[0]);
      returnSet.add(pairs[1]);
    }
    return returnSet;
  }

  @Override
  public boolean reload() throws SolrException {
    assert(gotArgs == true);
    assert(gotLoader == true);
    return true;
  }

  @Override
  public void init(Map<String,String> args) {
    assert(args.get("foo").equals("bar"));
    gotArgs = true;
    args.remove("foo");
  }

  @Override
  public void inform(ResourceLoader loader) throws SolrException {
    assert(loader != null);
    gotLoader = true;
    assert(gotArgs == true);
  }
  
}
