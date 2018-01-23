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

import org.apache.solr.common.SolrException;

import java.util.Currency;

/**
 * Represents a Currency field value, which includes a long amount and ISO currency code.
 */
public class CurrencyValue implements Comparable<CurrencyValue> {
  private long amount;
  private String currencyCode;

  /**
   * Constructs a new currency value.
   *
   * @param amount       The amount.
   * @param currencyCode The currency code.
   */
  public CurrencyValue(long amount, String currencyCode) {
    this.amount = amount;
    this.currencyCode = currencyCode;
  }

  /**
   * Constructs a new currency value by parsing the specific input.
   * <p>
   * Currency values are expected to be in the format &lt;amount&gt;,&lt;currency code&gt;,
   * for example, "500,USD" would represent 5 U.S. Dollars.
   * </p>
   * <p>
   * If no currency code is specified, the default is assumed.
   * </p>
   * @param externalVal The value to parse.
   * @param defaultCurrency The default currency.
   * @return The parsed CurrencyValue.
   */
  public static CurrencyValue parse(String externalVal, String defaultCurrency) {
    if (externalVal == null) {
      return null;
    }
    String amount = externalVal;
    String code = defaultCurrency;

    if (externalVal.contains(",")) {
      String[] amountAndCode = externalVal.split(",");
      amount = amountAndCode[0];
      code = amountAndCode[1];
    }

    if (amount.equals("*")) {
      return null;
    }

    Currency currency = CurrencyField.getCurrency(code);

    if (currency == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Currency code not supported by this JVM: " + code);
    }

    try {
      double value = Double.parseDouble(amount);
      long currencyValue = Math.round(value * Math.pow(10.0, currency.getDefaultFractionDigits()));

      return new CurrencyValue(currencyValue, code);
    } catch (NumberFormatException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  /**
   * The amount of the CurrencyValue.
   *
   * @return The amount.
   */
  public long getAmount() {
    return amount;
  }

  /**
   * The ISO currency code of the CurrencyValue.
   *
   * @return The currency code.
   */
  public String getCurrencyCode() {
    return currencyCode;
  }

  /**
   * Performs a currency conversion &amp; unit conversion.
   *
   * @param exchangeRates      Exchange rates to apply.
   * @param sourceCurrencyCode The source currency code.
   * @param sourceAmount       The source amount.
   * @param targetCurrencyCode The target currency code.
   * @return The converted indexable units after the exchange rate and currency fraction digits are applied.
   */
  public static long convertAmount(ExchangeRateProvider exchangeRates, String sourceCurrencyCode, long sourceAmount, String targetCurrencyCode) {
    double exchangeRate = exchangeRates.getExchangeRate(sourceCurrencyCode, targetCurrencyCode);
    return convertAmount(exchangeRate, sourceCurrencyCode, sourceAmount, targetCurrencyCode);
  }

  /**
   * Performs a currency conversion &amp; unit conversion.
   *
   * @param exchangeRate         Exchange rate to apply.
   * @param sourceFractionDigits The fraction digits of the source.
   * @param sourceAmount         The source amount.
   * @param targetFractionDigits The fraction digits of the target.
   * @return The converted indexable units after the exchange rate and currency fraction digits are applied.
   */
  public static long convertAmount(final double exchangeRate, final int sourceFractionDigits, final long sourceAmount, final int targetFractionDigits) {
    int digitDelta = targetFractionDigits - sourceFractionDigits;
    double value = ((double) sourceAmount * exchangeRate);

    if (digitDelta != 0) {
      if (digitDelta < 0) {
        for (int i = 0; i < -digitDelta; i++) {
          value *= 0.1;
        }
      } else {
        for (int i = 0; i < digitDelta; i++) {
          value *= 10.0;
        }
      }
    }

    return (long) value;
  }

  /**
   * Performs a currency conversion &amp; unit conversion.
   *
   * @param exchangeRate       Exchange rate to apply.
   * @param sourceCurrencyCode The source currency code.
   * @param sourceAmount       The source amount.
   * @param targetCurrencyCode The target currency code.
   * @return The converted indexable units after the exchange rate and currency fraction digits are applied.
   */
  public static long convertAmount(double exchangeRate, String sourceCurrencyCode, long sourceAmount, String targetCurrencyCode) {
    if (targetCurrencyCode.equals(sourceCurrencyCode)) {
      return sourceAmount;
    }

    int sourceFractionDigits = Currency.getInstance(sourceCurrencyCode).getDefaultFractionDigits();
    Currency targetCurrency = Currency.getInstance(targetCurrencyCode);
    int targetFractionDigits = targetCurrency.getDefaultFractionDigits();
    return convertAmount(exchangeRate, sourceFractionDigits, sourceAmount, targetFractionDigits);
  }

  /**
   * Returns a new CurrencyValue that is the conversion of this CurrencyValue to the specified currency.
   *
   * @param exchangeRates      The exchange rate provider.
   * @param targetCurrencyCode The target currency code to convert this CurrencyValue to.
   * @return The converted CurrencyValue.
   */
  public CurrencyValue convertTo(ExchangeRateProvider exchangeRates, String targetCurrencyCode) {
    return new CurrencyValue(convertAmount(exchangeRates, this.getCurrencyCode(), this.getAmount(), targetCurrencyCode), targetCurrencyCode);
  }

  /**
   * Returns a string representing the currency value such as "3.14,USD" for
   * a CurrencyValue of $3.14 USD.
   */
  public String strValue() {
    int digits = 0;
    try {
      Currency currency =
        Currency.getInstance(this.getCurrencyCode());
      if (currency == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Invalid currency code " + this.getCurrencyCode());
  }
      digits = currency.getDefaultFractionDigits();
}
    catch(IllegalArgumentException exception) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Invalid currency code " + this.getCurrencyCode());
    }

    String amount = Long.toString(this.getAmount());
    if (this.getAmount() == 0) {
      amount += "000000".substring(0,digits);
    }
    return
      amount.substring(0, amount.length() - digits)
      + "." + amount.substring(amount.length() - digits)
      + "," +  this.getCurrencyCode();
  }

  @Override
  public int compareTo(CurrencyValue o) {
    if(o == null) {
      throw new NullPointerException("Cannot compare CurrencyValue to a null values");
    }
    if(!getCurrencyCode().equals(o.getCurrencyCode())) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Cannot compare CurrencyValues when their currencies are not equal");
    }
    if(o.getAmount() < getAmount()) {
      return 1;
    }
    if(o.getAmount() == getAmount()) {
      return 0;
    }
    return -1;
  }

  @Override
  public String toString() {
    return strValue();
  }
}
