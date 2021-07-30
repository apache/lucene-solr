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
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.LongValueFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;

/**
 * <p>
 * Attempts to mutate selected fields that have only CharSequence-typed values
 * into Long values.  Grouping separators (',' in the ROOT locale) are parsed.
 * </p>
 * <p>
 * The default selection behavior is to mutate both those fields that don't match
 * a schema field, as well as those fields that match a schema field with a long 
 * field type.
 * </p>
 * <p>
 * If all values are parseable as long (or are already Long), then the field
 * will be mutated, replacing each value with its parsed Long equivalent;
 * otherwise, no mutation will occur.
 * </p>
 * <p>
 * The locale to use when parsing field values, which will affect the recognized
 * grouping separator character, may optionally be specified.  If no locale is
 * configured, then {@link Locale#ROOT} will be used. The following configuration
 * specifies the Russian/Russia locale, which will parse the string "12 345 899" 
 * as 12345899L (the grouping separator character is U+00AO NO-BREAK SPACE).
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.ParseLongFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="locale"&gt;ru_RU&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 *
 * <p>
 * See {@link Locale} for a description of acceptable language, country (optional)
 * and variant (optional) values, joined with underscore(s).
 * </p>
 * @since 4.4.0
 */
public class ParseLongFieldUpdateProcessorFactory extends ParseNumericFieldUpdateProcessorFactory {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return new ParseLongFieldUpdateProcessor(getSelector(), locale, next);
  }

  private static class ParseLongFieldUpdateProcessor extends AllValuesOrNoneFieldMutatingUpdateProcessor {
    private final Locale locale;
    
    // NumberFormat instances are not thread safe
    private final ThreadLocal<NumberFormat> numberFormat = new ThreadLocal<NumberFormat>() {
      @Override
      protected NumberFormat initialValue() {
        NumberFormat format = NumberFormat.getInstance(locale);
        format.setParseIntegerOnly(true);
        return format;
      }
    };

    ParseLongFieldUpdateProcessor(FieldNameSelector selector, Locale locale, UpdateRequestProcessor next) {
      super(selector, next);
      this.locale = locale;
    }
    
    @Override
    protected Object mutateValue(Object srcVal) {
      Object parsed = parsePossibleLong(srcVal, numberFormat.get());
      return parsed != null ? parsed : SKIP_FIELD_VALUE_LIST_SINGLETON;
    }
  }

  @Override
  protected boolean isSchemaFieldTypeCompatible(FieldType type) {
    return type instanceof LongValueFieldType;
  }

  public static Object parsePossibleLong(Object srcVal, NumberFormat numberFormat) {
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
      return number.longValue();
    }
    if (srcVal instanceof Long) {
      return srcVal;
    }
    return null;
  }
}
