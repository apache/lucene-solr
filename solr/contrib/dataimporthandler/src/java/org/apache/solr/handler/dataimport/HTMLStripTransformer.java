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

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;

import java.io.IOException;
import java.io.StringReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link Transformer} implementation which strip off HTML tags using {@link HTMLStripCharFilter} This is useful
 * in case you don't need this HTML anyway.
 *
 * @see HTMLStripCharFilter
 * @since solr 1.4
 */
public class HTMLStripTransformer extends Transformer {

  @Override
  @SuppressWarnings("unchecked")
  public Object transformRow(Map<String, Object> row, Context context) {
    List<Map<String, String>> fields = context.getAllEntityFields();
    for (Map<String, String> field : fields) {
      String col = field.get(DataImporter.COLUMN);
      String splitHTML = context.replaceTokens(field.get(STRIP_HTML));
      if (!TRUE.equals(splitHTML))
        continue;
      Object tmpVal = row.get(col);
      if (tmpVal == null)
        continue;

      if (tmpVal instanceof List) {
        List<String> inputs = (List<String>) tmpVal;
        @SuppressWarnings({"rawtypes"})
        List results = new ArrayList();
        for (String input : inputs) {
          if (input == null)
            continue;
          Object o = stripHTML(input, col);
          if (o != null)
            results.add(o);
        }
        row.put(col, results);
      } else {
        String value = tmpVal.toString();
        Object o = stripHTML(value, col);
        if (o != null)
          row.put(col, o);
      }
    }
    return row;
  }

  private Object stripHTML(String value, String column) {
    StringBuilder out = new StringBuilder();
    StringReader strReader = new StringReader(value);
    try {
      HTMLStripCharFilter html = new HTMLStripCharFilter(strReader.markSupported() ? strReader : new BufferedReader(strReader));
      char[] cbuf = new char[1024 * 10];
      while (true) {
        int count = html.read(cbuf);
        if (count == -1)
          break; // end of stream mark is -1
        if (count > 0)
          out.append(cbuf, 0, count);
      }
      html.close();
    } catch (IOException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Failed stripping HTML for column: " + column, e);
    }
    return out.toString();
  }

  public static final String STRIP_HTML = "stripHTML";

  public static final String TRUE = "true";
}
