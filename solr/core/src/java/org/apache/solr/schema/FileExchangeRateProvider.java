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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.SafeXMLParsing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Configuration for currency. Provides currency exchange rates.
 */
public class FileExchangeRateProvider implements ExchangeRateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final String PARAM_CURRENCY_CONFIG       = "currencyConfig";

  // Exchange rate map, maps Currency Code -> Currency Code -> Rate
  private Map<String, Map<String, Double>> rates = new HashMap<>();

  private String currencyConfigFile;
  private ResourceLoader loader;

  /**
   * Returns the currently known exchange rate between two currencies. If a direct rate has been loaded,
   * it is used. Otherwise, if a rate is known to convert the target currency to the source, the inverse
   * exchange rate is computed.
   *
   * @param sourceCurrencyCode The source currency being converted from.
   * @param targetCurrencyCode The target currency being converted to.
   * @return The exchange rate.
   * @throws SolrException if the requested currency pair cannot be found
   */
  @Override
  public double getExchangeRate(String sourceCurrencyCode, String targetCurrencyCode) {
    if (sourceCurrencyCode == null || targetCurrencyCode == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot get exchange rate; currency was null.");
    }
    
    if (sourceCurrencyCode.equals(targetCurrencyCode)) {
      return 1.0;
    }

    Double directRate = lookupRate(sourceCurrencyCode, targetCurrencyCode);

    if (directRate != null) {
      return directRate;
    }

    Double symmetricRate = lookupRate(targetCurrencyCode, sourceCurrencyCode);

    if (symmetricRate != null) {
      return 1.0 / symmetricRate;
    }

    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No available conversion rate between " + sourceCurrencyCode + " to " + targetCurrencyCode);
  }

  /**
   * Looks up the current known rate, if any, between the source and target currencies.
   *
   * @param sourceCurrencyCode The source currency being converted from.
   * @param targetCurrencyCode The target currency being converted to.
   * @return The exchange rate, or null if no rate has been registered.
   */
  private Double lookupRate(String sourceCurrencyCode, String targetCurrencyCode) {
    Map<String, Double> rhs = rates.get(sourceCurrencyCode);

    if (rhs != null) {
      return rhs.get(targetCurrencyCode);
    }

    return null;
  }

  /**
   * Registers the specified exchange rate.
   *
   * @param ratesMap           The map to add rate to
   * @param sourceCurrencyCode The source currency.
   * @param targetCurrencyCode The target currency.
   * @param rate               The known exchange rate.
   */
  private void addRate(Map<String, Map<String, Double>> ratesMap, String sourceCurrencyCode, String targetCurrencyCode, double rate) {
    Map<String, Double> rhs = ratesMap.get(sourceCurrencyCode);

    if (rhs == null) {
      rhs = new HashMap<>();
      ratesMap.put(sourceCurrencyCode, rhs);
    }

    rhs.put(targetCurrencyCode, rate);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FileExchangeRateProvider that = (FileExchangeRateProvider) o;

    return !(rates != null ? !rates.equals(that.rates) : that.rates != null);
  }

  @Override
  public int hashCode() {
    return rates != null ? rates.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "["+this.getClass().getName()+" : " + rates.size() + " rates.]";
  }

  @Override
  public Set<String> listAvailableCurrencies() {
    Set<String> currencies = new HashSet<>();
    for(Map.Entry<String, Map<String, Double>> entry : rates.entrySet()) {
      currencies.add(entry.getKey());
      for(String to : entry.getValue().keySet()) {
        currencies.add(to);
      }
    }
    return currencies;
  }

  @Override
  public boolean reload() throws SolrException {
    Map<String, Map<String, Double>> tmpRates = new HashMap<>();
    log.debug("Reloading exchange rates from file {}", currencyConfigFile);

    try {
      Document doc = SafeXMLParsing.parseConfigXML(log, loader, currencyConfigFile);
      XPathFactory xpathFactory = XPathFactory.newInstance();
      XPath xpath = xpathFactory.newXPath();
      
      // Parse exchange rates.
      NodeList nodes = (NodeList) xpath.evaluate("/currencyConfig/rates/rate", doc, XPathConstants.NODESET);
      
      for (int i = 0; i < nodes.getLength(); i++) {
        Node rateNode = nodes.item(i);
        NamedNodeMap attributes = rateNode.getAttributes();
        Node from = attributes.getNamedItem("from");
        Node to = attributes.getNamedItem("to");
        Node rate = attributes.getNamedItem("rate");
        
        if (from == null || to == null || rate == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exchange rate missing attributes (required: from, to, rate) " + rateNode);
        }
        
        String fromCurrency = from.getNodeValue();
        String toCurrency = to.getNodeValue();
        Double exchangeRate;
        
        if (null == CurrencyFieldType.getCurrency(fromCurrency)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Specified 'from' currency not supported in this JVM: " + fromCurrency);
        }
        if (null == CurrencyFieldType.getCurrency(toCurrency)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Specified 'to' currency not supported in this JVM: " + toCurrency);
        }
        
        try {
          exchangeRate = Double.parseDouble(rate.getNodeValue());
        } catch (NumberFormatException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not parse exchange rate: " + rateNode, e);
        }
        
        addRate(tmpRates, fromCurrency, toCurrency, exchangeRate);
      }
    } catch (SAXException | IOException | XPathExpressionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error while parsing currency configuration file "+currencyConfigFile, e);
    }
    // Atomically swap in the new rates map, if it loaded successfully
    this.rates = tmpRates;
    return true;
  }

  @Override
  public void init(Map<String,String> params) throws SolrException {
    this.currencyConfigFile = params.get(PARAM_CURRENCY_CONFIG);
    if(currencyConfigFile == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Missing required configuration "+PARAM_CURRENCY_CONFIG);
    }
    
    // Removing config params custom to us
    params.remove(PARAM_CURRENCY_CONFIG);
  }

  @Override
  public void inform(ResourceLoader loader) throws SolrException {
    if(loader == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Needs ResourceLoader in order to load config file");
    }
    this.loader = loader;
    reload();
  }
}
