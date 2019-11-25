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
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * <p>
 * A {@link Transformer} instance which can extract numbers out of strings. It uses
 * {@link NumberFormat} class to parse strings and supports
 * Number, Integer, Currency and Percent styles as supported by
 * {@link NumberFormat} with configurable locales.
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public class NumberFormatTransformer extends Transformer {

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object transformRow(Map<String, Object> row, Context context) {
    for (Map<String, String> fld : context.getAllEntityFields()) {
      String style = context.replaceTokens(fld.get(FORMAT_STYLE));
      if (style != null) {
        String column = fld.get(DataImporter.COLUMN);
        String srcCol = fld.get(RegexTransformer.SRC_COL_NAME);
        String localeStr = context.replaceTokens(fld.get(LOCALE));
        if (srcCol == null)
          srcCol = column;
        Locale locale = Locale.ROOT;
        if (localeStr != null) {
          try {
            locale = new Locale.Builder().setLanguageTag(localeStr).build();
          } catch (IllformedLocaleException e) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                "Invalid Locale '" + localeStr + "' specified for field: " + fld, e);
          }
        }

        Object val = row.get(srcCol);
        String styleSmall = style.toLowerCase(Locale.ROOT);

        if (val instanceof List) {
          List<String> inputs = (List) val;
          List results = new ArrayList();
          for (String input : inputs) {
            try {
              results.add(process(input, styleSmall, locale));
            } catch (ParseException e) {
              throw new DataImportHandlerException(
                      DataImportHandlerException.SEVERE,
                      "Failed to apply NumberFormat on column: " + column, e);
            }
          }
          row.put(column, results);
        } else {
          if (val == null || val.toString().trim().equals(""))
            continue;
          try {
            row.put(column, process(val.toString(), styleSmall, locale));
          } catch (ParseException e) {
            throw new DataImportHandlerException(
                    DataImportHandlerException.SEVERE,
                    "Failed to apply NumberFormat on column: " + column, e);
          }
        }
      }
    }
    return row;
  }

  private Number process(String val, String style, Locale locale) throws ParseException {
    if (INTEGER.equals(style)) {
      return parseNumber(val, NumberFormat.getIntegerInstance(locale));
    } else if (NUMBER.equals(style)) {
      return parseNumber(val, NumberFormat.getNumberInstance(locale));
    } else if (CURRENCY.equals(style)) {
      return parseNumber(val, NumberFormat.getCurrencyInstance(locale));
    } else if (PERCENT.equals(style)) {
      return parseNumber(val, NumberFormat.getPercentInstance(locale));
    }

    return null;
  }

  private Number parseNumber(String val, NumberFormat numFormat) throws ParseException {
    ParsePosition parsePos = new ParsePosition(0);
    Number num = numFormat.parse(val, parsePos);
    if (parsePos.getIndex() != val.length()) {
      throw new ParseException("illegal number format", parsePos.getIndex());
    }
    return num;
  }

  public static final String FORMAT_STYLE = "formatStyle";

  public static final String LOCALE = "locale";

  public static final String NUMBER = "number";

  public static final String PERCENT = "percent";

  public static final String INTEGER = "integer";

  public static final String CURRENCY = "currency";
}
