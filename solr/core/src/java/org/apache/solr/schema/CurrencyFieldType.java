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
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Field type for support of monetary values.
 * <p>
 * See <a href="http://wiki.apache.org/solr/CurrencyField">http://wiki.apache.org/solr/CurrencyField</a>
 */
public class CurrencyFieldType extends FieldType implements SchemaAware, ResourceLoaderAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected static final String PARAM_DEFAULT_CURRENCY = "defaultCurrency";
  protected static final String DEFAULT_DEFAULT_CURRENCY = "USD";
  protected static final String PARAM_RATE_PROVIDER_CLASS = "providerClass";
  protected static final String DEFAULT_RATE_PROVIDER_CLASS = "solr.FileExchangeRateProvider";
  protected static final String PARAM_FIELD_SUFFIX_AMOUNT_RAW = "amountLongSuffix";
  protected static final String PARAM_FIELD_SUFFIX_CURRENCY = "codeStrSuffix";

  protected IndexSchema schema;
  protected FieldType fieldTypeCurrency;
  protected FieldType fieldTypeAmountRaw;
  protected String fieldSuffixAmountRaw;
  protected String fieldSuffixCurrency;
  
  private String exchangeRateProviderClass;
  private String defaultCurrency;
  private ExchangeRateProvider provider;

  /**
   * A wrapper around <code>Currency.getInstance</code> that returns null
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

  /** The identifier code for the default currency of this field type */
  public String getDefaultCurrency() {
    return defaultCurrency;
  }


  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    if (this.isMultiValued()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " types can not be multiValued: " + this.typeName);
    }
    this.schema = schema;

    this.defaultCurrency = args.get(PARAM_DEFAULT_CURRENCY);
    if (this.defaultCurrency == null) {
      this.defaultCurrency = DEFAULT_DEFAULT_CURRENCY;
    } else {
      args.remove(PARAM_DEFAULT_CURRENCY);
    }
    if (null == getCurrency(this.defaultCurrency)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Default currency code is not supported by this JVM: " + this.defaultCurrency);
    }

    this.exchangeRateProviderClass = args.get(PARAM_RATE_PROVIDER_CLASS);
    if (this.exchangeRateProviderClass == null) {
      this.exchangeRateProviderClass = DEFAULT_RATE_PROVIDER_CLASS;
    } else {
      args.remove(PARAM_RATE_PROVIDER_CLASS);
    }
    try {
      Class<? extends ExchangeRateProvider> c
          = schema.getResourceLoader().findClass(exchangeRateProviderClass, ExchangeRateProvider.class);
      provider = c.newInstance();
      provider.init(args);
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Error instantiating exchange rate provider " + exchangeRateProviderClass + ": " + e.getMessage(), e);
    }

    if (fieldTypeAmountRaw == null) {      // Don't initialize if subclass already has done so
      fieldSuffixAmountRaw = args.get(PARAM_FIELD_SUFFIX_AMOUNT_RAW);
      if (fieldSuffixAmountRaw == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Missing required param " + PARAM_FIELD_SUFFIX_AMOUNT_RAW);
      } else {
        args.remove(PARAM_FIELD_SUFFIX_AMOUNT_RAW);
      }
    }
    
    if (fieldTypeCurrency == null) {       // Don't initialize if subclass already has done so
      fieldSuffixCurrency = args.get(PARAM_FIELD_SUFFIX_CURRENCY);
      if (fieldSuffixCurrency == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Missing required param " + PARAM_FIELD_SUFFIX_CURRENCY);
      } else {
        args.remove(PARAM_FIELD_SUFFIX_CURRENCY);
      }
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
          getClass().getSimpleName() + " fields can not be multiValued: " + field.getName());
    }
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object externalVal) {
    CurrencyValue value = CurrencyValue.parse(externalVal.toString(), defaultCurrency);

    List<IndexableField> f = new ArrayList<>();
    SchemaField amountField = getAmountField(field);
    f.add(amountField.createField(String.valueOf(value.getAmount())));
    SchemaField currencyField = getCurrencyField(field);
    f.add(currencyField.createField(value.getCurrencyCode()));

    if (field.stored()) {
      String storedValue = externalVal.toString().trim();
      if (storedValue.indexOf(",") < 0) {
        storedValue += "," + defaultCurrency;
      }
      f.add(createField(field.getName(), storedValue, StoredField.TYPE));
    }

    return f;
  }
  
  private SchemaField getAmountField(SchemaField field) {
    return schema.getField(field.getName() + POLY_FIELD_SEPARATOR + fieldSuffixAmountRaw);
  }

  private SchemaField getCurrencyField(SchemaField field) {
    return schema.getField(field.getName() + POLY_FIELD_SEPARATOR + fieldSuffixCurrency);
  }

  /**
   * When index schema is informed, get field types for the configured dynamic sub-fields 
   *
   * {@inheritDoc}
   *
   * @param schema {@inheritDoc}
   */
  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
    if (null == fieldTypeAmountRaw) {
      assert null != fieldSuffixAmountRaw : "How did we get here?";
      SchemaField field = schema.getFieldOrNull(POLY_FIELD_SEPARATOR + fieldSuffixAmountRaw);
      if (field == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Field type \"" + this.getTypeName()
            + "\": Undefined dynamic field for " + PARAM_FIELD_SUFFIX_AMOUNT_RAW + "=\"" + fieldSuffixAmountRaw + "\"");
      }
      fieldTypeAmountRaw = field.getType();
      if (!(fieldTypeAmountRaw instanceof LongValueFieldType)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Field type \"" + this.getTypeName()
            + "\": Dynamic field for " + PARAM_FIELD_SUFFIX_AMOUNT_RAW + "=\"" + fieldSuffixAmountRaw
            + "\" must have type class extending LongValueFieldType");
      }
    }
    if (null == fieldTypeCurrency) {
      assert null != fieldSuffixCurrency : "How did we get here?";
      SchemaField field = schema.getFieldOrNull(POLY_FIELD_SEPARATOR + fieldSuffixCurrency);
      if (field == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Field type \"" + this.getTypeName()
            + "\": Undefined dynamic field for " + PARAM_FIELD_SUFFIX_CURRENCY + "=\"" + fieldSuffixCurrency + "\"");
      }
      fieldTypeCurrency = field.getType();
      if (!(fieldTypeCurrency instanceof StrField)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Field type \"" + this.getTypeName()
            + "\": Dynamic field for " + PARAM_FIELD_SUFFIX_CURRENCY + "=\"" + fieldSuffixCurrency
            + "\" must have type class of (or extending) StrField");
      }
    }
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

    return getRangeQueryInternal(parser, field, valueDefault, valueDefault, true, true);
  }

  /**
   * <p>
   * Returns a ValueSource over this field in which the numeric value for 
   * each document represents the indexed value as converted to the default 
   * currency for the field, normalized to its most granular form based 
   * on the default fractional digits.
   * </p>
   * <p>
   * For example: If the default Currency specified for a field is 
   * <code>USD</code>, then the values returned by this value source would 
   * represent the equivalent number of "cents" (ie: value in dollars * 100)
   * after converting each document's native currency to USD -- because the 
   * default fractional digits for <code>USD</code> is "<code>2</code>".  
   * So for a document whose indexed value was currently equivalent to
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
    getAmountField(field).checkFieldCacheSource();
    getCurrencyField(field).checkFieldCacheSource();
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
   * represent the equivalent number of dollars after converting each
   * document's raw value to <code>USD</code>.  So for a document whose 
   * indexed value was currently equivalent to "<code>5.43,USD</code>"
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

  /**
   * Override the default existenceQuery implementation to run an existence query on the underlying amountField instead.
   */
  @Override
  public Query getExistenceQuery(QParser parser, SchemaField field) {
    // Use an existence query of the underlying amount field
    SchemaField amountField = getAmountField(field);
    return amountField.getType().getExistenceQuery(parser, amountField);
  }

  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2, final boolean minInclusive, final boolean maxInclusive) {
    final CurrencyValue p1 = CurrencyValue.parse(part1, defaultCurrency);
    final CurrencyValue p2 = CurrencyValue.parse(part2, defaultCurrency);

    if (p1 != null && p2 != null && !p1.getCurrencyCode().equals(p2.getCurrencyCode())) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Cannot parse range query " + part1 + " to " + part2 +
              ": range queries only supported when upper and lower bound have same currency.");
    }

    return getRangeQueryInternal(parser, field, p1, p2, minInclusive, maxInclusive);
  }

  private Query getRangeQueryInternal(QParser parser, SchemaField field, final CurrencyValue p1, final CurrencyValue p2, final boolean minInclusive, final boolean maxInclusive) {
    String currencyCode = (p1 != null) ? p1.getCurrencyCode() :
        (p2 != null) ? p2.getCurrencyCode() : defaultCurrency;

    // ValueSourceRangeFilter doesn't check exists(), so we have to
    final Query docsWithValues = new DocValuesFieldExistsQuery(getAmountField(field).getName());
    final Query vsRangeFilter = new ValueSourceRangeFilter
        (new RawCurrencyValueSource(field, currencyCode, parser),
            p1 == null ? null : p1.getAmount() + "",
            p2 == null ? null : p2.getAmount() + "",
            minInclusive, maxInclusive);
    return new ConstantScoreQuery(new BooleanQuery.Builder()
        .add(docsWithValues, Occur.FILTER)
        .add(vsRangeFilter, Occur.FILTER).build());
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
  public void write(TextResponseWriter writer, String name, IndexableField field) throws IOException {
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
    public FunctionValues getValues(@SuppressWarnings({"rawtypes"})Map context, LeafReaderContext reader)
        throws IOException {
      final FunctionValues amounts = source.getValues(context, reader);
      // the target digits & currency of our source, 
      // become the source digits & currency of ourselves
      final String sourceCurrencyCode = source.getTargetCurrency().getCurrencyCode();
      final double divisor = Math.pow(10D, targetCurrency.getDefaultFractionDigits());
      return new FunctionValues() {
        @Override
        public boolean exists(int doc) throws IOException {
          return amounts.exists(doc);
        }
        @Override
        public long longVal(int doc) throws IOException {
          return (long) doubleVal(doc);
        }
        @Override
        public int intVal(int doc) throws IOException {
          return (int) doubleVal(doc);
        }

        @Override
        public double doubleVal(int doc) throws IOException {
          return CurrencyValue.convertAmount(rate, sourceCurrencyCode, amounts.longVal(doc), targetCurrency.getCurrencyCode()) / divisor;
        }

        @Override
        public float floatVal(int doc) throws IOException {
          return CurrencyValue.convertAmount(rate, sourceCurrencyCode, amounts.longVal(doc), targetCurrency.getCurrencyCode()) / ((float)divisor);
        }

        @Override
        public String strVal(int doc) throws IOException {
          return Double.toString(doubleVal(doc));
        }

        @Override
        public String toString(int doc) throws IOException {
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
   * (ie: <code>$n * 100</code>) since the number of default fractional 
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

      SchemaField amountField = getAmountField(sf);
      SchemaField currencyField = getCurrencyField(sf);

      currencyValues = currencyField.getType().getValueSource(currencyField, parser);
      amountValues = amountField.getType().getValueSource(amountField, parser);
    }

    public SchemaField getField() { return sf; }
    public Currency getTargetCurrency() { return targetCurrency; }

    @Override
    @SuppressWarnings({"unchecked"})
    public FunctionValues getValues(@SuppressWarnings({"rawtypes"})Map context, LeafReaderContext reader) throws IOException {
      final FunctionValues amounts = amountValues.getValues(context, reader);
      final FunctionValues currencies = currencyValues.getValues(context, reader);

      return new FunctionValues() {
        private static final int MAX_CURRENCIES_TO_CACHE = 256;
        private final int[] fractionDigitCache = new int[MAX_CURRENCIES_TO_CACHE];
        private final String[] currencyOrdToCurrencyCache = new String[MAX_CURRENCIES_TO_CACHE];
        private final double[] exchangeRateCache = new double[MAX_CURRENCIES_TO_CACHE];
        private int targetFractionDigits = -1;
        private int targetCurrencyOrd = -1;
        private boolean initializedCache;

        private String getDocCurrencyCode(int doc, int currencyOrd) throws IOException {
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
        private Currency getDocCurrency(int doc, int currencyOrd) throws IOException {
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
        public boolean exists(int doc) throws IOException {
          return amounts.exists(doc);
        }

        @Override
        public long longVal(int doc) throws IOException {
          long amount = amounts.longVal(doc);
          // bail fast using whatever amounts defaults to if no value
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
        public int intVal(int doc) throws IOException {
          return (int) longVal(doc);
        }

        @Override
        public double doubleVal(int doc) throws IOException {
          return (double) longVal(doc);
        }

        @Override
        public float floatVal(int doc) throws IOException {
          return (float) longVal(doc);
        }

        @Override
        public String strVal(int doc) throws IOException {
          return Long.toString(longVal(doc));
        }

        @Override
        public String toString(int doc) throws IOException {
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

