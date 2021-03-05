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

package org.apache.solr.prometheus.exporter;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricsQueryTemplate {
  /*
  A regex with named groups is used to match template references to template + vars using the basic pattern:

      $jq:<TEMPLATE>( <UNIQUE>, <KEYSELECTOR>, <METRIC>, <TYPE> )

  For instance,

      $jq:core(requests_total, endswith(".requestTimes"), count, COUNTER)

  TEMPLATE = core
  UNIQUE = requests_total (unique suffix for this metric, results in a metric named "solr_metrics_core_requests_total")
  KEYSELECTOR = endswith(".requestTimes") (filter to select the specific key for this metric)
  METRIC = count
  TYPE = COUNTER
  */
  private static final Pattern matchJqTemplate =
      Pattern.compile("^\\$jq:(?<TEMPLATE>.*?)\\(\\s?(?<UNIQUE>[^,]*),\\s?(?<KEYSELECTOR>[^,]*)(,\\s?(?<METRIC>[^,]*)\\s?)?(,\\s?(?<TYPE>[^,]*)\\s?)?\\)$");

  public static Optional<Matcher> matches(String jsonQuery) {
    Optional<Matcher> maybe = Optional.empty();
    if (jsonQuery != null) {
      String toMatch = jsonQuery.replaceAll("\\s+", " ").trim();
      Matcher m = matchJqTemplate.matcher(toMatch);
      if (m.matches()) {
        maybe = Optional.of(m);
      }
    }
    return maybe;
  }

  private final String name;
  private final String defaultType;
  private final String template;

  public MetricsQueryTemplate(String name, String template, String defaultType) {
    Objects.requireNonNull(name, "jq template must have a name");
    Objects.requireNonNull(template, "jq template is required");

    this.name = name;
    this.template = template.replaceAll("\\s+", " ").trim();
    if (this.template.isEmpty()) {
      throw new IllegalArgumentException("jq template must not be empty");
    }
    this.defaultType = defaultType != null ? defaultType : "GAUGE";
  }

  public String getName() {
    return name;
  }

  public String applyTemplate(final Matcher matched) {
    String keySelector = matched.group("KEYSELECTOR");
    if (keySelector == null) keySelector = "";

    String unique = matched.group("UNIQUE");
    unique = unique != null ? unique.trim() : "";

    String type = matched.group("TYPE");
    if (type == null) {
      type = defaultType;
    }

    String metric = matched.group("METRIC");
    if (metric == null) {
      metric = unique;
    }

    // could be a simple field name or some kind of function here
    if (!metric.contains("$")) {
      if ("object.value".equals(metric)) {
        metric = "$object.value"; // don't require the user to supply the $
      } else if (!metric.contains("$object")) {
        if (Character.isDigit(metric.charAt(0))) {
          // jq doesn't like fields that start with a number
          metric = "$object.value[\"" + metric + "\"]";
        } else {
          // just a simple field reference in the value object
          metric = "$object.value." + metric;
        }
      } // else some kind of function, pass thru as-is
    } // else there's a $ so just assume it is a fully qualified reference to the desired value, leave as-is

    return template
        .replace("{UNIQUE}", unique)
        .replace("{KEYSELECTOR}", keySelector.trim())
        .replace("{METRIC}", metric.trim())
        .replace("{TYPE}", type.trim());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MetricsQueryTemplate that = (MetricsQueryTemplate) o;
    return name.equals(that.name) &&
        Objects.equals(defaultType, that.defaultType) &&
        template.equals(that.template);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, defaultType, template);
  }
}
