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
package org.apache.solr.handler.dataimport;

import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * {@link Transformer} instance which creates {@link Date} instances out of {@link String}s.
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DateFormatTransformer extends Transformer {
  private Map<String, SimpleDateFormat> fmtCache = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  @SuppressWarnings("unchecked")
  public Object transformRow(Map<String, Object> aRow, Context context) {

    for (Map<String, String> map : context.getAllEntityFields()) {
      Locale locale = Locale.ENGLISH; // we default to ENGLISH for dates for full Java 9 compatibility
      String customLocale = map.get(LOCALE);
      if (customLocale != null) {
        try {
          locale = new Locale.Builder().setLanguageTag(customLocale).build();
        } catch (IllformedLocaleException e) {
          throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Invalid Locale specified: " + customLocale, e);
        }
      }

      String fmt = map.get(DATE_TIME_FMT);
      if (fmt == null)
        continue;
      VariableResolver resolver = context.getVariableResolver();
      fmt = resolver.replaceTokens(fmt);
      String column = map.get(DataImporter.COLUMN);
      String srcCol = map.get(RegexTransformer.SRC_COL_NAME);
      if (srcCol == null)
        srcCol = column;
      try {
        Object o = aRow.get(srcCol);
        if (o instanceof List) {
          @SuppressWarnings({"rawtypes"})
          List inputs = (List) o;
          List<Date> results = new ArrayList<>();
          for (Object input : inputs) {
            results.add(process(input, fmt, locale));
          }
          aRow.put(column, results);
        } else {
          if (o != null) {
            aRow.put(column, process(o, fmt, locale));
          }
        }
      } catch (ParseException e) {
        log.warn("Could not parse a Date field ", e);
      }
    }
    return aRow;
  }

  private Date process(Object value, String format, Locale locale) throws ParseException {
    if (value == null) return null;
    String strVal = value.toString().trim();
    if (strVal.length() == 0)
      return null;
    SimpleDateFormat fmt = fmtCache.get(format);
    if (fmt == null) {
      fmt = new SimpleDateFormat(format, locale);
      fmtCache.put(format, fmt);
    }
    return fmt.parse(strVal);
  }

  public static final String DATE_TIME_FMT = "dateTimeFormat";
  
  public static final String LOCALE = "locale";
}
