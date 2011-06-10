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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * Provides functionality for replacing variables in a templatized string. It
 * can also be used to get the place-holders (variables) in a templatized
 * string.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 *
 * @since solr 1.3
 */
public class TemplateString {
  private List<String> variables = new ArrayList<String>();

  private List<String> pcs = new ArrayList<String>();

  private Map<String, TemplateString> cache;

  public TemplateString() {
    cache = new ConcurrentHashMap<String, TemplateString>();
  }

  private TemplateString(String s) {
    Matcher m = WORD_PATTERN.matcher(s);
    int idx = 0;
    while (m.find()) {
      String aparam = s.substring(m.start() + 2, m.end() - 1);
      variables.add(aparam);
      pcs.add(s.substring(idx, m.start()));
      idx = m.end();
    }
    pcs.add(s.substring(idx));
  }

  /**
   * Returns a string with all variables replaced by the known values. An
   * unknown variable is replaced by an empty string.
   *
   * @param string the String to be resolved
   * @param resolver the VariableResolver instance to be used for evaluation
   * @return the string with all variables replaced
   */
  public String replaceTokens(String string, VariableResolver resolver) {
    if (string == null)
      return null;
    TemplateString ts = cache.get(string);
    if (ts == null) {
      ts = new TemplateString(string);
      cache.put(string, ts);
    }
    return ts.fillTokens(resolver);
  }

  private String fillTokens(VariableResolver resolver) {
    String[] s = new String[variables.size()];
    for (int i = 0; i < variables.size(); i++) {
      Object val = resolver.resolve(variables.get(i));
      s[i] = val == null ? "" : getObjectAsString(val);
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < pcs.size(); i++) {
      sb.append(pcs.get(i));
      if (i < s.length) {
        sb.append(s[i]);
      }
    }

    return sb.toString();
  }

  private String getObjectAsString(Object val) {
    if (val instanceof Date) {
      Date d = (Date) val;
      return DataImporter.DATE_TIME_FORMAT.get().format(d);
    }
    return val.toString();
  }

  /**
   * Returns the variables in the given string.
   *
   * @param s the templatized string
   * @return the list of variables (strings) in the given templatized string.
   */
  public static List<String> getVariables(String s) {
    return new TemplateString(s).variables;
  }

  static final Pattern WORD_PATTERN = Pattern.compile("(\\$\\{.*?\\})");
}
