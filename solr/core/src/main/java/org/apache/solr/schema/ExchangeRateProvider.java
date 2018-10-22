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
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.SolrException;

/**
 * Interface for providing pluggable exchange rate providers to @CurrencyField
 */
public interface ExchangeRateProvider {
  /**
   * Get the exchange rate between the two given currencies
   * @return the exchange rate as a double
   * @throws SolrException if the rate is not defined in the provider
   */
  public double getExchangeRate(String sourceCurrencyCode, String targetCurrencyCode) throws SolrException;
  
  /**
   * List all configured currency codes which are valid as source/target for this Provider
   * @return a Set of <a href="http://en.wikipedia.org/wiki/ISO_4217">ISO 4217</a> currency code strings
   */
  public Set<String> listAvailableCurrencies();

  /**
   * Ask the currency provider to explicitly reload/refresh its configuration.
   * If this does not make sense for a particular provider, simply do nothing
   * @throws SolrException if there is a problem reloading
   * @return true if reload of rates succeeded, else false
   */
  public boolean reload() throws SolrException;

  /**
   * Initializes the provider by passing in a set of key/value configs as a map.
   * Note that the map also contains other fieldType parameters, so make sure to
   * avoid name clashes.
   * <p>
   * Important: Custom config params must be removed from the map before returning
   * @param args a @Map of key/value config params to initialize the provider
   */
  public void init(Map<String,String> args);

  /**
   * Passes a ResourceLoader, used to read config files from e.g. ZooKeeper.
   * Implementations not needing resource loader can implement this as NOOP.
   * <p>Typically called after init
   * @param loader a @ResourceLoader instance
   */
  public void inform(ResourceLoader loader) throws SolrException;
}
