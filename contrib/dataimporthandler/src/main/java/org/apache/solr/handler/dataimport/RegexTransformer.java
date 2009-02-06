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

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * A Transformer implementation which uses Regular Expressions to extract, split
 * and replace data in fields.
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
public class RegexTransformer extends Transformer {
  private static final Logger LOG = LoggerFactory.getLogger(RegexTransformer.class);

  @SuppressWarnings("unchecked")
  public Map<String, Object> transformRow(Map<String, Object> row,
                                          Context context) {
    VariableResolver vr = context.getVariableResolver();
    List<Map<String, String>> fields = context.getAllEntityFields();
    for (Map<String, String> field : fields) {
      String col = field.get(DataImporter.COLUMN);
      String reStr = field.get(REGEX);
      reStr = vr.replaceTokens(reStr);
      String splitBy = field.get(SPLIT_BY);
      splitBy =  vr.replaceTokens(splitBy);
      String replaceWith = field.get(REPLACE_WITH);
      replaceWith = vr.replaceTokens(replaceWith);
      if (reStr != null || splitBy != null) {
        String srcColName = field.get(SRC_COL_NAME);
        if (srcColName == null) {
          srcColName = col;
        }
        Object tmpVal = row.get(srcColName);
        if (tmpVal == null)
          continue;

        if (tmpVal instanceof List) {
          List<String> inputs = (List<String>) tmpVal;
          List results = new ArrayList();
          for (String input : inputs) {
            Object o = process(col, reStr, splitBy, replaceWith, input);
            if (o != null)
              results.add(o);
          }
          row.put(col, results);
        } else {
          String value = tmpVal.toString();
          Object o = process(col, reStr, splitBy, replaceWith, value);
          if (o != null)
            row.put(col, o);
        }
      }
    }
    return row;
  }

  private Object process(String col, String reStr, String splitBy,
                         String replaceWith, String value) {
    if (splitBy != null) {
      return readBySplit(splitBy, value);
    } else if (replaceWith != null) {
      Pattern p = getPattern(reStr);
      return p.matcher(value).replaceAll(replaceWith);
    } else {
      return readfromRegExp(reStr, value, col);
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> readBySplit(String splitBy, String value) {
    String[] vals = value.split(splitBy);
    List<String> l = new ArrayList<String>();
    l.addAll(Arrays.asList(vals));
    return l;
  }

  @SuppressWarnings("unchecked")
  private Object readfromRegExp(String reStr, String value, String columnName) {
    Pattern regexp = getPattern(reStr);
    Matcher m = regexp.matcher(value);
    if (m.find() && m.groupCount() > 0) {
      if (m.groupCount() > 1) {
        List l = new ArrayList();
        for (int i = 1; i <= m.groupCount(); i++) {
          try {
            l.add(m.group(i));
          } catch (Exception e) {
            LOG.warn("Parsing failed for field : " + columnName, e);
          }
        }
        return l;
      } else {
        return m.group(1);
      }
    }

    return null;
  }

  private Pattern getPattern(String reStr) {
    Pattern result = PATTERN_CACHE.get(reStr);
    if (result == null) {
      PATTERN_CACHE.put(reStr, result = Pattern.compile(reStr));
    }
    return result;
  }

  private HashMap<String, Pattern> PATTERN_CACHE = new HashMap<String, Pattern>();

  public static final String REGEX = "regex";

  public static final String REPLACE_WITH = "replaceWith";

  public static final String SPLIT_BY = "splitBy";

  public static final String SRC_COL_NAME = "sourceColName";

}
