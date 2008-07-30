package org.apache.solr.handler.dataimport;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * A Transformer instance which can extract numbers out of strings. It uses
 * <code>java.text.NumberFormat</code> class to parse strings and supports
 * Number, Integer, Currency and Percent styles as supported by
 * <code>java.text.NumberFormat</code>
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

  @SuppressWarnings("unchecked")
  public Object transformRow(Map<String, Object> row, Context context) {
    for (Map<String, String> fld : context.getAllEntityFields()) {
      String style = fld.get(FORMAT_STYLE);
      if (style != null) {
        String column = fld.get(DataImporter.COLUMN);
        String srcCol = fld.get(RegexTransformer.SRC_COL_NAME);
        if (srcCol == null)
          srcCol = column;

        Object val = row.get(srcCol);
        String styleSmall = style.toLowerCase();

        if (val instanceof List) {
          List<String> inputs = (List) val;
          List results = new ArrayList();
          for (String input : inputs) {
            try {
              results.add(process(input, styleSmall));
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
            row.put(column, process(val.toString(), styleSmall));
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

  private Number process(String val, String style) throws ParseException {
    if (INTEGER.equals(style)) {
      return NumberFormat.getIntegerInstance().parse(val);
    } else if (NUMBER.equals(style)) {
      return NumberFormat.getNumberInstance().parse(val);
    } else if (CURRENCY.equals(style)) {
      return NumberFormat.getCurrencyInstance().parse(val);
    } else if (PERCENT.equals(style)) {
      return NumberFormat.getPercentInstance().parse(val);
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
