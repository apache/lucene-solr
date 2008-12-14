/**
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Transformer instance which creates Date instances out of Strings.
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class DateFormatTransformer extends Transformer {
  private static final Logger LOG = LoggerFactory
          .getLogger(DateFormatTransformer.class);

  @SuppressWarnings("unchecked")
  public Object transformRow(Map<String, Object> aRow, Context context) {
    for (Map<String, String> map : context.getAllEntityFields()) {
      String fmt = map.get(DATE_TIME_FMT);
      if (fmt == null)
        continue;
      String column = map.get(DataImporter.COLUMN);
      String srcCol = map.get(RegexTransformer.SRC_COL_NAME);
      if (srcCol == null)
        srcCol = column;
      try {
        Object o = aRow.get(srcCol);
        if (o instanceof List) {
          List<String> inputs = (List<String>) o;
          List<Date> results = new ArrayList<Date>();
          for (String input : inputs) {
            results.add(process(input, fmt));
          }
          aRow.put(column, results);
        } else {
          if (o != null)  {
            aRow.put(column, process(o.toString(), fmt));
          }
        }
      } catch (ParseException e) {
        LOG.warn( "Could not parse a Date field ", e);
      }
    }
    return aRow;
  }

  private Date process(String value, String format) throws ParseException {
    if (value == null || value.trim().length() == 0)
      return null;

    return new SimpleDateFormat(format).parse(value);
  }

  public static final String DATE_TIME_FMT = "dateTimeFormat";
}
