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

package org.apache.solr.common.util;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Template {
  public final String template;
  public static final Pattern DOLLAR_BRACES_PLACEHOLDER_PATTERN = Pattern
      .compile("[$][{](.*?)[}]");
  public static final Pattern BRACES_PLACEHOLDER_PATTERN = Pattern
      .compile("[{](.*?)[}]");


  public Template(String template, Pattern pattern) {
    this.template = template;
    List<String> variables = new ArrayList<>(2);
    Matcher m = pattern.matcher(template);
    while (m.find()) {
      String variable = m.group(1);
      startIndexes.add(m.start(0));
      endOffsets.add(m.end(0));
      variables.add(variable);
    }
    this.variables = Collections.unmodifiableList(variables);

  }

  public String apply(Function<String, Object> valueSupplier) {
    if (startIndexes != null) {
      StringBuilder sb = new StringBuilder(template);
      for (int i = startIndexes.size() - 1; i >= 0; i--) {
        String replacement = valueSupplier.apply(variables.get(i)).toString();
        sb.replace(startIndexes.get(i), endOffsets.get(i), replacement);
      }
      return sb.toString();
    } else {
      return template;
    }
  }

  private List<Integer> startIndexes = new ArrayList<>(2);
  private List<Integer> endOffsets = new ArrayList<>(2);
  public final List<String> variables ;
}
