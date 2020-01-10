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

package org.apache.solr.client.solrj.routing;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.StrUtils;

public class PreferenceRule {
  public final String name;
  public final String value;

  public PreferenceRule(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public static List<PreferenceRule> from(String rules) {
    List<String> prefs = StrUtils.splitSmart(rules, ',');
    ArrayList<PreferenceRule> preferenceRules = new ArrayList<>(prefs.size());
    prefs.forEach(rule -> {
      String[] parts = rule.split(":", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid " + ShardParams.SHARDS_PREFERENCE + " rule: " + rule);
      }
      preferenceRules.add(new PreferenceRule(parts[0], parts[1]));
    });
    return preferenceRules;
  }
}
