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

package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.Cache;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.ConcurrentLRUCache;

/**
* Adds new fields to documents based on a template pattern specified via Template.field
* request parameters (multi-valued) or 'field' value specified in initArgs.
* <p>
* The format of the parameter is &lt;field-name&gt;:&lt;the-template-string&gt;, for example: <br>
* <b>Template.field=fname:${somefield}some_string${someotherfield}</b>
*
*/
public class TemplateUpdateProcessorFactory extends SimpleUpdateProcessorFactory {
  private Cache<String, Resolved> templateCache = new ConcurrentLRUCache<>(1000, 800, 900, 10, false, false, null);
  @Override
  protected void process(AddUpdateCommand cmd, SolrQueryRequest req, SolrQueryResponse rsp) {
    String[] vals = getParams("field");
    SolrInputDocument doc = cmd.getSolrInputDocument();
    if (vals != null && vals.length > 0) {
      for (String val : vals) {
        if (val == null || val.isEmpty()) continue;
        int idx = val.indexOf(':');
        if (idx == -1)
          throw new RuntimeException("'field' must be of the format <field-name>:<the-template-string>");

        String fName = val.substring(0, idx);
        String template = val.substring(idx + 1);
        doc.addField(fName, replaceTokens(template, templateCache, s -> {
          Object v = doc.getFieldValue(s);
          return v == null ? "" : v;
        }));
      }
    }

  }


  public static Resolved getResolved(String template, Cache<String, Resolved> cache) {
    Resolved r = cache == null ? null : cache.get(template);
    if (r == null) {
      r = new Resolved();
      Matcher m = PLACEHOLDER_PATTERN.matcher(template);
      while (m.find()) {
        String variable = m.group(1);
        r.startIndexes.add(m.start(0));
        r.endOffsets.add(m.end(0));
        r.variables.add(variable);
      }
      if (cache != null) cache.put(template, r);
    }
    return r;
  }

  /**
   * Get a list of variables embedded in the template string.
   */
  public static List<String> getVariables(String template, Cache<String, Resolved> cache) {
    Resolved r = getResolved(template, cache);
    if (r == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(r.variables);
  }

  public static String replaceTokens(String template, Cache<String, Resolved> cache, Function<String, Object> fun) {
    if (template == null) {
      return null;
    }
    Resolved r = getResolved(template, cache);
    if (r.startIndexes != null) {
      StringBuilder sb = new StringBuilder(template);
      for (int i = r.startIndexes.size() - 1; i >= 0; i--) {
        String replacement = fun.apply(r.variables.get(i)).toString();
        sb.replace(r.startIndexes.get(i), r.endOffsets.get(i), replacement);
      }
      return sb.toString();
    } else {
      return template;
    }
  }


  public static class Resolved {
    public List<Integer> startIndexes = new ArrayList<>(2);
    public List<Integer> endOffsets = new ArrayList<>(2);
    public List<String> variables = new ArrayList<>(2);
  }

  public static final Pattern PLACEHOLDER_PATTERN = Pattern
      .compile("[$][{](.*?)[}]");
}
