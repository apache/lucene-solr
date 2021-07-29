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
package org.apache.solr.update.processor;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DoubleValueFieldType;
import org.apache.solr.schema.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;

/**
 * <p>
 * Attempts to mutate selected fields that have only CharSequence-typed values
 * into Double values.  If required, rounding uses ceiling mode:
 * {@link RoundingMode#CEILING}.  Grouping separators (',' in the ROOT locale)
 * are parsed.
 * </p>
 * <p>
 * The default selection behavior is to mutate both those fields that don't match
 * a schema field, as well as those fields that match a schema field with a double
 * field type.
 * </p>
 * <p>
 * If all values are parseable as double (or are already Double), then the field
 * will be mutated, replacing each value with its parsed Double equivalent; 
 * otherwise, no mutation will occur.
 * </p>
 * <p>
 * The locale to use when parsing field values, which will affect the recognized
 * grouping separator and decimal characters, may optionally be specified.  If
 * no locale is configured, then {@link Locale#ROOT} will be used.  The following
 * configuration specifies the Russian/Russia locale, which will parse the string
 * string "12 345,899" as double value 12345.899 (the grouping separator
 * character is U+00AO NO-BREAK SPACE).
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.ParseDoubleFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="locale"&gt;ru_RU&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 *
 * <p>
 * See {@link Locale} for a description of acceptable language, country (optional)
 * and variant (optional) values, joined with underscore(s).
 * </p>
 * @since 4.4.0
 */
public class ParseDoubleFieldUpdateProcessorFactory extends ParseNumericFieldUpdateProcessorFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return new ParseDoubleFieldUpdateProcessor(getSelector(), locale, next); 
  }

  private static final class ParseDoubleFieldUpdateProcessor extends AllValuesOrNoneFieldMutatingUpdateProcessor {
    private final Locale locale;
    // NumberFormat instances are not thread safe
    private final ThreadLocal<NumberFormat> numberFormat = new ThreadLocal<NumberFormat>() {
      @Override
      protected NumberFormat initialValue() {
        NumberFormat format = NumberFormat.getInstance(locale);
        format.setParseIntegerOnly(false);
        format.setRoundingMode(RoundingMode.CEILING);
        return format;
      }
    };

    ParseDoubleFieldUpdateProcessor(FieldNameSelector selector, Locale locale, UpdateRequestProcessor next) {
      super(selector, next);
      this.locale = locale;
    }

    @Override
    protected Object mutateValue(Object srcVal) {
      Object parsed = parsePossibleDouble(srcVal, numberFormat.get());
      return parsed != null ? parsed : SKIP_FIELD_VALUE_LIST_SINGLETON;
    }
  }

  @Override
  protected boolean isSchemaFieldTypeCompatible(FieldType type) {
    return type instanceof DoubleValueFieldType;
  }

  public static Object parsePossibleDouble(Object srcVal, NumberFormat numberFormat) {
    if (srcVal instanceof CharSequence) {
      String stringVal = srcVal.toString();
      ParsePosition pos = new ParsePosition(0);
      Number number = numberFormat.parse(stringVal, pos);
      if (pos.getIndex() != stringVal.length()) {
        if (log.isDebugEnabled()) {
          log.debug("value '{}' is not parseable, thus not mutated; unparsed chars: '{}'",
              srcVal, stringVal.substring(pos.getIndex()));
        }
        return null;
      }
      return number.doubleValue();
    }
    if (srcVal instanceof Double) {
      return srcVal;
    }
    if (srcVal instanceof Float) {
      return ((Float) srcVal).doubleValue();
    }
    return null;
  }
}
