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

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FieldValueFilter;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Currency;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Field type for support of monetary values.
 * <p>
 * See <a href="http://wiki.apache.org/solr/CurrencyField">http://wiki.apache.org/solr/CurrencyField</a>
 */
public class CurrencyField extends FieldType implements SchemaAware, ResourceLoaderAware {
  protected static final String PARAM_DEFAULT_CURRENCY      = "defaultCurrency";
  protected static final String PARAM_RATE_PROVIDER_CLASS   = "providerClass";
  protected static final Object PARAM_PRECISION_STEP        = "precisionStep";
  protected static final String DEFAULT_RATE_PROVIDER_CLASS = "solr.FileExchangeRateProvider";
  protected static final String DEFAULT_DEFAULT_CURRENCY    = "USD";
  protected static final String DEFAULT_PRECISION_STEP      = "0";
  protected static final String FIELD_SUFFIX_AMOUNT_RAW     = "_amount_raw";
  protected static final String FIELD_SUFFIX_CURRENCY       = "_currency";

  private IndexSchema schema;
  protected FieldType fieldTypeCurrency;
  protected FieldType fieldTypeAmountRaw;
  private String exchangeRateProviderClass;
  private String defaultCurrency;
  private ExchangeRateProvider provider;
  public static Logger log = LoggerFactory.getLogger(CurrencyField.class);

  /**
   * A wrapper arround <code>Currency.getInstance</code> that returns null
   * instead of throwing <code>IllegalArgumentException</code>
   * if the specified Currency does not exist in this JVM.
   *
   * @see Currency#getInstance(String)
   */
  public static Currency getCurrency(final String code) {
    try {
      return Currency.getInstance(code);
    } catch (IllegalArgumentException e) {
      /* :NOOP: */
    }
    return null;
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    if (this.isMultiValued()) { 
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                              "CurrencyField types can not be multiValued: " + 
                              this.typeName);
    }
    this.schema = schema;
    this.exchangeRateProviderClass = args.get(PARAM_RATE_PROVIDER_CLASS);
    this.defaultCurrency = args.get(PARAM_DEFAULT_CURRENCY);

    if (this.defaultCurrency == null) {
      this.defaultCurrency = DEFAULT_DEFAULT_CURRENCY;
    }
    
    if (this.exchangeRateProviderClass == null) {
      this.exchangeRateProviderClass = DEFAULT_RATE_PROVIDER_CLASS;
    }

    if (null == getCurrency(this.defaultCurrency)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Default currency code is not supported by this JVM: " + this.defaultCurrency);
    }

    String precisionStepString = args.get(PARAM_PRECISION_STEP);
    if (precisionStepString == null) {
      precisionStepString = DEFAULT_PRECISION_STEP;
    }

    // Initialize field type for amount
    fieldTypeAmountRaw = new TrieLongField();
    fieldTypeAmountRaw.setTypeName("amount_raw_type_tlong");
    Map<String,String> map = new HashMap<>(1);
    map.put("precisionStep", precisionStepString);
    fieldTypeAmountRaw.init(schema, map);
    
    // Initialize field type for currency string
    fieldTypeCurrency = new StrField();
    fieldTypeCurrency.setTypeName("currency_type_string");
    fieldTypeCurrency.init(schema, new HashMap<String,String>());
    
    args.remove(PARAM_RATE_PROVIDER_CLASS);
    args.remove(PARAM_DEFAULT_CURRENCY);
    args.remove(PARAM_PRECISION_STEP);

    try {
      Class<? extends ExchangeRateProvider> c = schema.getResourceLoader().findClass(exchangeRateProviderClass, ExchangeRateProvider.class);
      provider = c.newInstance();
      provider.init(args);
    } catch (Exception e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Error instantiating exchange rate provider "+exchangeRateProviderClass+": " + e.getMessage(), e);
    }
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  @Override
  public void checkSchemaField(final SchemaField field) throws SolrException {
    super.checkSchemaField(field);
    if (field.multiValued()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                              "CurrencyFields can not be multiValued: " + 
                              field.getName());
    }
  }

  @Override
  public List<StorableField> createFields(SchemaField field, Object externalVal, float boost) {
    CurrencyValue value = CurrencyValue.parse(externalVal.toString(), defaultCurrency);

    List<StorableField> f = new ArrayList<>();
    SchemaField amountField = getAmountField(field);
    f.add(amountField.createField(String.valueOf(value.getAmount()), amountField.indexed() && !amountField.omitNorms() ? boost : 1F));
    SchemaField currencyField = getCurrencyField(field);
    f.add(currencyField.createField(value.getCurrencyCode(), currencyField.indexed() && !currencyField.omitNorms() ? boost : 1F));

    if (field.stored()) {
      org.apache.lucene.document.FieldType customType = new org.apache.lucene.document.FieldType();
      assert !customType.omitNorms();
      customType.setStored(true);
      String storedValue = externalVal.toString().trim();
      if (storedValue.indexOf(",") < 0) {
        storedValue += "," + defaultCurrency;
      }
      f.add(createField(field.getName(), storedValue, customType, 1F));
    }

    return f;
  }

  private SchemaField getAmountField(SchemaField field) {
    return schema.getField(field.getName() + POLY_FIELD_SEPARATOR + FIELD_SUFFIX_AMOUNT_RAW);
  }

  private SchemaField getCurrencyField(SchemaField field) {
    return schema.getField(field.getName() + POLY_FIELD_SEPARATOR + FIELD_SUFFIX_CURRENCY);
  }

  private void createDynamicCurrencyField(String suffix, FieldType type) {
    String name = "*" + POLY_FIELD_SEPARATOR + suffix;
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");
    props.put("stored", "false");
    props.put("multiValued", "false");
    props.put("omitNorms", "true");
    int p = SchemaField.calcProps(name, type, props);
    schema.registerDynamicFields(SchemaField.create(name, type, p, null));
  }

  /**
   * When index schema is informed, add dynamic fields "*____currency" and "*____amount_raw". 
   * 
   * {@inheritDoc}
   * 
   * @param schema {@inheritDoc}
   */
  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
    createDynamicCurrencyField(FIELD_SUFFIX_CURRENCY,   fieldTypeCurrency);
    createDynamicCurrencyField(FIELD_SUFFIX_AMOUNT_RAW, fieldTypeAmountRaw);
  }

  /**
   * Load the currency config when resource loader initialized.
   *
   * @param resourceLoader The resource loader.
   */
  @Override
  public void inform(ResourceLoader resourceLoader) {
    provider.inform(resourceLoader);
    boolean reloaded = provider.reload();
    if(!reloaded) {
      log.warn("Failed reloading currencies");
    }
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    CurrencyValue value = CurrencyValue.parse(externalVal, defaultCurrency);
    CurrencyValue valueDefault;
    valueDefault = value.convertTo(provider, defaultCurrency);

    return getRangeQuery(parser, field, valueDefault, valueDefault, true, true);
  }

  /**
   * <p>
   * Returns a ValueSource over this field in which the numeric value for 
   * each document represents the indexed value as converted to the default 
   * currency for the field, normalized to it's most granular form based 
   * on the default fractional digits.
   * </p>
   * <p>
   * For example: If the default Currency specified for a field is 
   * <code>USD</code>, then the values returned by this value source would 
   * represent the equivilent number of "cents" (ie: value in dollars * 100) 
   * after converting each document's native currency to USD -- because the 
   * default fractional digits for <code>USD</code> is "<code>2</code>".  
   * So for a document whose indexed value was currently equivilent to 
   * "<code>5.43,USD</code>" using the the exchange provider for this field, 
   * this ValueSource would return a value of "<code>543</code>"
   * </p>
   *
   * @see #PARAM_DEFAULT_CURRENCY
   * @see #DEFAULT_DEFAULT_CURRENCY
   * @see Currency#getDefaultFractionDigits
   * @see #getConvertedValueSource
   */
  public RawCurrencyValueSource getValueSource(SchemaField field, 
                                               QParser parser) {
    field.checkFieldCacheSource(parser);
    return new RawCurrencyValueSource(field, defaultCurrency, parser);
  }

  /**
   * <p>
   * Returns a ValueSource over this field in which the numeric value for 
   * each document represents the value from the underlying 
   * <code>RawCurrencyValueSource</code> as converted to the specified target 
   * Currency.
   * </p>
   * <p>
   * For example: If the <code>targetCurrencyCode</code> param is set to
   * <code>USD</code>, then the values returned by this value source would 
   * represent the equivilent number of dollars after converting each 
   * document's raw value to <code>USD</code>.  So for a document whose 
   * indexed value was currently equivilent to "<code>5.43,USD</code>" 
   * using the the exchange provider for this field, this ValueSource would 
   * return a value of "<code>5.43</code>"
   * </p>
   *
   * @param targetCurrencyCode The target currency for the resulting value source, if null the defaultCurrency for this field type will be used
   * @param source the raw ValueSource to wrap
   * @see #PARAM_DEFAULT_CURRENCY
   * @see #DEFAULT_DEFAULT_CURRENCY
   * @see #getValueSource
   */
  public ValueSource getConvertedValueSource(String targetCurrencyCode, 
                                             RawCurrencyValueSource source) {
    if (null == targetCurrencyCode) { 
      targetCurrencyCode = defaultCurrency; 
    }
    return new ConvertedCurrencyValueSource(targetCurrencyCode, 
                                            source);
  }

  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, final boolean minInclusive, final boolean maxInclusive) {
      final CurrencyValue p1 = CurrencyValue.parse(part1, defaultCurrency);
      final CurrencyValue p2 = CurrencyValue.parse(part2, defaultCurrency);

      if (p1 != null && p2 != null && !p1.getCurrencyCode().equals(p2.getCurrencyCode())) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "Cannot parse range query " + part1 + " to " + part2 +
                          ": range queries only supported when upper and lower bound have same currency.");
      }

      return getRangeQuery(parser, field, p1, p2, minInclusive, maxInclusive);
  }

  public Query getRangeQuery(QParser parser, SchemaField field, final CurrencyValue p1, final CurrencyValue p2, final boolean minInclusive, final boolean maxInclusive) {
    String currencyCode = (p1 != null) ? p1.getCurrencyCode() :
                          (p2 != null) ? p2.getCurrencyCode() : defaultCurrency;

    // ValueSourceRangeFilter doesn't check exists(), so we have to
    final Filter docsWithValues = new FieldValueFilter(getAmountField(field).getName());
    final Filter vsRangeFilter = new ValueSourceRangeFilter
      (new RawCurrencyValueSource(field, currencyCode, parser),
       p1 == null ? null : p1.getAmount() + "", 
       p2 == null ? null : p2.getAmount() + "",
       minInclusive, maxInclusive);
    final BooleanFilter docsInRange = new BooleanFilter();
    docsInRange.add(docsWithValues, Occur.MUST);
    docsInRange.add(vsRangeFilter, Occur.MUST);

    return new SolrConstantScoreQuery(docsInRange);
    
  }

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    // Convert all values to default currency for sorting.
    return (new RawCurrencyValueSource(field, defaultCurrency, null)).getSortField(reverse);
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public void write(TextResponseWriter writer, String name, StorableField field) throws IOException {
    writer.writeStr(name, field.stringValue(), true);
  }

  public ExchangeRateProvider getProvider() {
    return provider;
  }

  /**
   * <p>
   * A value source whose values represent the "normal" values
   * in the specified target currency.
   * </p>
   * @see RawCurrencyValueSource
   */
  class ConvertedCurrencyValueSource extends ValueSource {
    private final Currency targetCurrency;
    private final RawCurrencyValueSource source;
    private final double rate;
    public ConvertedCurrencyValueSource(String targetCurrencyCode, 
                                        RawCurrencyValueSource source) {
      this.source = source;
      this.targetCurrency = getCurrency(targetCurrencyCode);
      if (null == targetCurrency) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Currency code not supported by this JVM: " + targetCurrencyCode);
      }
      // the target digits & currency of our source, 
      // become the source digits & currency of ourselves
      this.rate = provider.getExchangeRate
        (source.getTargetCurrency().getCurrencyCode(), 
         targetCurrency.getCurrencyCode());
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext reader) 
      throws IOException {
      final FunctionValues amounts = source.getValues(context, reader);
      // the target digits & currency of our source, 
      // become the source digits & currency of ourselves
      final String sourceCurrencyCode = source.getTargetCurrency().getCurrencyCode();
      final int sourceFractionDigits = source.getTargetCurrency().getDefaultFractionDigits();
      final double divisor = Math.pow(10D, targetCurrency.getDefaultFractionDigits());
      return new FunctionValues() {
        @Override
        public boolean exists(int doc) {
          return amounts.exists(doc);
        }
        @Override
        public long longVal(int doc) {
          return (long) doubleVal(doc);
        }
        @Override
        public int intVal(int doc) {
          return (int) doubleVal(doc);
        }

        @Override
        public double doubleVal(int doc) {
          return CurrencyValue.convertAmount(rate, sourceCurrencyCode, amounts.longVal(doc), targetCurrency.getCurrencyCode()) / divisor;
        }

        @Override
        public float floatVal(int doc) {
          return CurrencyValue.convertAmount(rate, sourceCurrencyCode, amounts.longVal(doc), targetCurrency.getCurrencyCode()) / ((float)divisor);
        }

        @Override
        public String strVal(int doc) {
          return Double.toString(doubleVal(doc));
        }

        @Override
        public String toString(int doc) {
          return name() + '(' + strVal(doc) + ')';
        }
      };
    }
    public String name() {
      return "currency";
    }

    @Override
    public String description() {
      return name() + "(" + source.getField().getName() + "," + targetCurrency.getCurrencyCode()+")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConvertedCurrencyValueSource that = (ConvertedCurrencyValueSource) o;

      return !(source != null ? !source.equals(that.source) : that.source != null) &&
        (rate == that.rate) && 
        !(targetCurrency != null ? !targetCurrency.equals(that.targetCurrency) : that.targetCurrency != null);

    }

    @Override
    public int hashCode() {
      int result = targetCurrency != null ? targetCurrency.hashCode() : 0;
      result = 31 * result + (source != null ? source.hashCode() : 0);
      result = 31 * (int) Double.doubleToLongBits(rate);
      return result;
    }
  }

  /**
   * <p>
   * A value source whose values represent the "raw" (ie: normalized using 
   * the number of default fractional digits) values in the specified 
   * target currency).
   * </p>
   * <p>
   * For example: if the specified target currency is "<code>USD</code>" 
   * then the numeric values are the number of pennies in the value 
   * (ie: <code>$n * 100</code>) since the number of defalt fractional 
   * digits for <code>USD</code> is "<code>2</code>")
   * </p>
   * @see ConvertedCurrencyValueSource
   */
  class RawCurrencyValueSource extends ValueSource {
    private static final long serialVersionUID = 1L;
    private final Currency targetCurrency;
    private ValueSource currencyValues;
    private ValueSource amountValues;
    private final SchemaField sf;

    public RawCurrencyValueSource(SchemaField sfield, String targetCurrencyCode, QParser parser) {
      this.sf = sfield;
      this.targetCurrency = getCurrency(targetCurrencyCode);
      if (null == targetCurrency) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Currency code not supported by this JVM: " + targetCurrencyCode);
      }

      SchemaField amountField = schema.getField(sf.getName() + POLY_FIELD_SEPARATOR + FIELD_SUFFIX_AMOUNT_RAW);
      SchemaField currencyField = schema.getField(sf.getName() + POLY_FIELD_SEPARATOR + FIELD_SUFFIX_CURRENCY);

      currencyValues = currencyField.getType().getValueSource(currencyField, parser);
      amountValues = amountField.getType().getValueSource(amountField, parser);
    }
    
    public SchemaField getField() { return sf; }
    public Currency getTargetCurrency() { return targetCurrency; }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext reader) throws IOException {
      final FunctionValues amounts = amountValues.getValues(context, reader);
      final FunctionValues currencies = currencyValues.getValues(context, reader);

      return new FunctionValues() {
        private final int MAX_CURRENCIES_TO_CACHE = 256;
        private final int[] fractionDigitCache = new int[MAX_CURRENCIES_TO_CACHE];
        private final String[] currencyOrdToCurrencyCache = new String[MAX_CURRENCIES_TO_CACHE];
        private final double[] exchangeRateCache = new double[MAX_CURRENCIES_TO_CACHE];
        private int targetFractionDigits = -1;
        private int targetCurrencyOrd = -1;
        private boolean initializedCache;

        private String getDocCurrencyCode(int doc, int currencyOrd) {
          if (currencyOrd < MAX_CURRENCIES_TO_CACHE) {
            String currency = currencyOrdToCurrencyCache[currencyOrd];

            if (currency == null) {
              currencyOrdToCurrencyCache[currencyOrd] = currency = currencies.strVal(doc);
            }
            
            if (currency == null) {
              currency = defaultCurrency;
            }

            if (targetCurrencyOrd == -1 && 
                currency.equals(targetCurrency.getCurrencyCode() )) {
              targetCurrencyOrd = currencyOrd;
            }

            return currency;
          } else {
            return currencies.strVal(doc);
          }
        }
        /** throws a (Server Error) SolrException if the code is not valid */
        private Currency getDocCurrency(int doc, int currencyOrd) {
          String code = getDocCurrencyCode(doc, currencyOrd);
          Currency c = getCurrency(code);
          if (null == c) {
            throw new SolrException
              (SolrException.ErrorCode.SERVER_ERROR, 
               "Currency code of document is not supported by this JVM: "+code);
          }
          return c;
        }

        @Override
        public boolean exists(int doc) {
          return amounts.exists(doc);
        }
        
        @Override
        public long longVal(int doc) {
          long amount = amounts.longVal(doc);
          // bail fast using whatever ammounts defaults to if no value
          // (if we don't do this early, currencyOrd may be < 0, 
          // causing index bounds exception
          if ( ! exists(doc) ) {
            return amount;
          }

          if (!initializedCache) {
            for (int i = 0; i < fractionDigitCache.length; i++) {
              fractionDigitCache[i] = -1;
            }

            initializedCache = true;
          }

          int currencyOrd = currencies.ordVal(doc);

          if (currencyOrd == targetCurrencyOrd) {
            return amount;
          }

          double exchangeRate;
          int sourceFractionDigits;

          if (targetFractionDigits == -1) {
            targetFractionDigits = targetCurrency.getDefaultFractionDigits();
          }

          if (currencyOrd < MAX_CURRENCIES_TO_CACHE) {
            exchangeRate = exchangeRateCache[currencyOrd];

            if (exchangeRate <= 0.0) {
              String sourceCurrencyCode = getDocCurrencyCode(doc, currencyOrd);
              exchangeRate = exchangeRateCache[currencyOrd] = provider.getExchangeRate(sourceCurrencyCode, targetCurrency.getCurrencyCode());
            }

            sourceFractionDigits = fractionDigitCache[currencyOrd];

            if (sourceFractionDigits == -1) {
              sourceFractionDigits = fractionDigitCache[currencyOrd] = getDocCurrency(doc, currencyOrd).getDefaultFractionDigits();
            }
          } else {
            Currency source = getDocCurrency(doc, currencyOrd);
            exchangeRate = provider.getExchangeRate(source.getCurrencyCode(), targetCurrency.getCurrencyCode());
            sourceFractionDigits = source.getDefaultFractionDigits();
          }

          return CurrencyValue.convertAmount(exchangeRate, sourceFractionDigits, amount, targetFractionDigits);
        }

        @Override
        public int intVal(int doc) {
          return (int) longVal(doc);
        }

        @Override
        public double doubleVal(int doc) {
          return (double) longVal(doc);
        }

        @Override
        public float floatVal(int doc) {
          return (float) longVal(doc);
        }

        @Override
        public String strVal(int doc) {
          return Long.toString(longVal(doc));
        }

        @Override
        public String toString(int doc) {
          return name() + '(' + amounts.toString(doc) + ',' + currencies.toString(doc) + ')';
        }
      };
    }

    public String name() {
      return "rawcurrency";
    }

    @Override
    public String description() {
      return name() + "(" + sf.getName() + 
        ",target="+targetCurrency.getCurrencyCode()+")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RawCurrencyValueSource that = (RawCurrencyValueSource) o;

      return !(amountValues != null ? !amountValues.equals(that.amountValues) : that.amountValues != null) &&
              !(currencyValues != null ? !currencyValues.equals(that.currencyValues) : that.currencyValues != null) &&
              !(targetCurrency != null ? !targetCurrency.equals(that.targetCurrency) : that.targetCurrency != null);

    }

    @Override
    public int hashCode() {
      int result = targetCurrency != null ? targetCurrency.hashCode() : 0;
      result = 31 * result + (currencyValues != null ? currencyValues.hashCode() : 0);
      result = 31 * result + (amountValues != null ? amountValues.hashCode() : 0);
      return result;
    }
  }
}

/**
 * Configuration for currency. Provides currency exchange rates.
 */
class FileExchangeRateProvider implements ExchangeRateProvider {
  public static Logger log = LoggerFactory.getLogger(FileExchangeRateProvider.class);
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
    for(String from : rates.keySet()) {
      currencies.add(from);
      for(String to : rates.get(from).keySet()) {
        currencies.add(to);
      }
    }
    return currencies;
  }

  @Override
  public boolean reload() throws SolrException {
    InputStream is = null;
    Map<String, Map<String, Double>> tmpRates = new HashMap<>();
    try {
      log.info("Reloading exchange rates from file "+this.currencyConfigFile);

      is = loader.openResource(currencyConfigFile);
      javax.xml.parsers.DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      try {
        dbf.setXIncludeAware(true);
        dbf.setNamespaceAware(true);
      } catch (UnsupportedOperationException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "XML parser doesn't support XInclude option", e);
      }
      
      try {
        Document doc = dbf.newDocumentBuilder().parse(is);
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
          
          if (null == CurrencyField.getCurrency(fromCurrency)) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Specified 'from' currency not supported in this JVM: " + fromCurrency);
          }
          if (null == CurrencyField.getCurrency(toCurrency)) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Specified 'to' currency not supported in this JVM: " + toCurrency);
          }
          
          try {
            exchangeRate = Double.parseDouble(rate.getNodeValue());
          } catch (NumberFormatException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not parse exchange rate: " + rateNode, e);
          }
          
          addRate(tmpRates, fromCurrency, toCurrency, exchangeRate);
        }
      } catch (SAXException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error parsing currency config.", e);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error parsing currency config.", e);
      } catch (ParserConfigurationException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error parsing currency config.", e);
      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error parsing currency config.", e);
      }
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error while opening Currency configuration file "+currencyConfigFile, e);
    } finally {
      try {
        if (is != null) {
          is.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    // Atomically swap in the new rates map, if it loaded successfully
    this.rates = tmpRates;
    return true;
  }

  @Override
  public void init(Map<String,String> params) throws SolrException {
    this.currencyConfigFile = params.get(PARAM_CURRENCY_CONFIG);
    if(currencyConfigFile == null) {
      throw new SolrException(ErrorCode.NOT_FOUND, "Missing required configuration "+PARAM_CURRENCY_CONFIG);
    }
    
    // Removing config params custom to us
    params.remove(PARAM_CURRENCY_CONFIG);
  }

  @Override
  public void inform(ResourceLoader loader) throws SolrException {
    if(loader == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Needs ResourceLoader in order to load config file");
    }
    this.loader = loader;
    reload();
  }
}

/**
 * Represents a Currency field value, which includes a long amount and ISO currency code.
 */
class CurrencyValue {
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
   * <p/>
   * Currency values are expected to be in the format &lt;amount&gt;,&lt;currency code&gt;,
   * for example, "500,USD" would represent 5 U.S. Dollars.
   * <p/>
   * If no currency code is specified, the default is assumed.
   *
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
   * Performs a currency conversion & unit conversion.
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
   * Performs a currency conversion & unit conversion.
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
   * Performs a currency conversion & unit conversion.
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

  @Override
  public String toString() {
    return String.valueOf(amount) + "," + currencyCode;
  }
}
