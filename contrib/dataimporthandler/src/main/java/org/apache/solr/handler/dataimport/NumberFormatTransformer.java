package org.apache.solr.handler.dataimport;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * A Transformer instance which can extract numbers out of strings. It uses
 * <code>java.text.NumberFormat</code> class to parse strings and supports
 * Number, Integer, Currency and Percent styles as supported by
 * <code>java.text.NumberFormat</code> with configurable locales.
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class NumberFormatTransformer extends Transformer {

  private static final Pattern localeRegex = Pattern.compile("^([a-z]{2})-([A-Z]{2})$");

  @SuppressWarnings("unchecked")
  public Object transformRow(Map<String, Object> row, Context context) {
    for (Map<String, String> fld : context.getAllEntityFields()) {
      String style = fld.get(FORMAT_STYLE);
      if (style != null) {
        String column = fld.get(DataImporter.COLUMN);
        String srcCol = fld.get(RegexTransformer.SRC_COL_NAME);
        Locale locale = null;
        String localeStr = fld.get(LOCALE);
        if (srcCol == null)
          srcCol = column;
        if (localeStr != null) {
          Matcher matcher = localeRegex.matcher(localeStr);
          if (matcher.find() && matcher.groupCount() == 2) {
            locale = new Locale(matcher.group(1), matcher.group(2));
          } else {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Invalid Locale specified for field: " + fld);
          }
        } else {
          locale = Locale.getDefault();
        }

        Object val = row.get(srcCol);
        String styleSmall = style.toLowerCase();

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
      return NumberFormat.getIntegerInstance(locale).parse(val);
    } else if (NUMBER.equals(style)) {
      return NumberFormat.getNumberInstance(locale).parse(val);
    } else if (CURRENCY.equals(style)) {
      return NumberFormat.getCurrencyInstance(locale).parse(val);
    } else if (PERCENT.equals(style)) {
      return NumberFormat.getPercentInstance(locale).parse(val);
    }

    return null;
  }

  public static final String FORMAT_STYLE = "formatStyle";

  public static final String LOCALE = "locale";

  public static final String NUMBER = "number";

  public static final String PERCENT = "percent";

  public static final String INTEGER = "integer";

  public static final String CURRENCY = "currency";
}
