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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * A set of nested maps that can resolve variables by namespaces. Variables are
 * enclosed with a dollar sign then an opening curly brace, ending with a
 * closing curly brace. Namespaces are delimited with '.' (period).
 * </p>
 * <p>
 * This class also has special logic to resolve evaluator calls by recognizing
 * the reserved function namespace: dataimporter.functions.xxx
 * </p>
 * <p>
 * This class caches strings that have already been resolved from the current
 * dih import.
 * </p>
 * <b>This API is experimental and may change in the future.</b>
 * 
 * 
 * @since solr 1.3
 */
public class VariableResolver {
  
  private static final Pattern DOT_PATTERN = Pattern.compile("[.]");
  private static final Pattern PLACEHOLDER_PATTERN = Pattern
      .compile("[$][{](.*?)[}]");
  private static final Pattern EVALUATOR_FORMAT_PATTERN = Pattern
      .compile("^(\\w*?)\\((.*?)\\)$");
  private Map<String,Object> rootNamespace;
  private Map<String,Evaluator> evaluators;
  private Map<String,Resolved> cache = new WeakHashMap<>();
  
  class Resolved {
    List<Integer> startIndexes = new ArrayList<>(2);
    List<Integer> endOffsets = new ArrayList<>(2);
    List<String> variables = new ArrayList<>(2);
  }
  
  public static final String FUNCTIONS_NAMESPACE = "dataimporter.functions.";
  public static final String FUNCTIONS_NAMESPACE_SHORT = "dih.functions.";
  
  public VariableResolver() {
    rootNamespace = new HashMap<>();
  }
  
  public VariableResolver(Properties defaults) {
    rootNamespace = new HashMap<>();
    for (Map.Entry<Object,Object> entry : defaults.entrySet()) {
      rootNamespace.put(entry.getKey().toString(), entry.getValue());
    }
  }
  
  public VariableResolver(Map<String,Object> defaults) {
    rootNamespace = new HashMap<>(defaults);
  }
  
  /**
   * Resolves a given value with a name
   * 
   * @param name
   *          the String to be resolved
   * @return an Object which is the result of evaluation of given name
   */
  public Object resolve(String name) {
    Object r = null;
    if (name != null) {
      String[] nameParts = DOT_PATTERN.split(name);
      CurrentLevel cr = currentLevelMap(nameParts,
          rootNamespace, false);
      Map<String,Object> currentLevel = cr.map;
      r = currentLevel.get(nameParts[nameParts.length - 1]);
      if (r == null && name.startsWith(FUNCTIONS_NAMESPACE)
          && name.length() > FUNCTIONS_NAMESPACE.length()) {
        return resolveEvaluator(FUNCTIONS_NAMESPACE, name);
      }
      if (r == null && name.startsWith(FUNCTIONS_NAMESPACE_SHORT)
          && name.length() > FUNCTIONS_NAMESPACE_SHORT.length()) {
        return resolveEvaluator(FUNCTIONS_NAMESPACE_SHORT, name);
      }
      if (r == null) {
        StringBuilder sb = new StringBuilder();
        for(int i=cr.level ; i<nameParts.length ; i++) {
          if(sb.length()>0) {
            sb.append(".");
          }
          sb.append(nameParts[i]);
        }
        r = cr.map.get(sb.toString());
      }      
      if (r == null) {
        r = System.getProperty(name);
      }
    }
    return r == null ? "" : r;
  }
  
  private Object resolveEvaluator(String namespace, String name) {
    if (evaluators == null) {
      return "";
    }
    Matcher m = EVALUATOR_FORMAT_PATTERN.matcher(name
        .substring(namespace.length()));
    if (m.find()) {
      String fname = m.group(1);
      Evaluator evaluator = evaluators.get(fname);
      if (evaluator == null) return "";
      ContextImpl ctx = new ContextImpl(null, this, null, null, null, null,
          null);
      String g2 = m.group(2);
      return evaluator.evaluate(g2, ctx);
    } else {
      return "";
    }
  }
  
  /**
   * Given a String with place holders, replace them with the value tokens.
   * 
   * @return the string with the placeholders replaced with their values
   */
  public String replaceTokens(String template) {
    if (template == null) {
      return null;
    }
    Resolved r = getResolved(template);
    if (r.startIndexes != null) {
      StringBuilder sb = new StringBuilder(template);
      for (int i = r.startIndexes.size() - 1; i >= 0; i--) {
        String replacement = resolve(r.variables.get(i)).toString();
        sb.replace(r.startIndexes.get(i), r.endOffsets.get(i), replacement);
      }
      return sb.toString();
    } else {
      return template;
    }
  }
  
  private Resolved getResolved(String template) {
    Resolved r = cache.get(template);
    if (r == null) {
      r = new Resolved();
      Matcher m = PLACEHOLDER_PATTERN.matcher(template);
      while (m.find()) {
        String variable = m.group(1);
        r.startIndexes.add(m.start(0));
        r.endOffsets.add(m.end(0));
        r.variables.add(variable);
      }
      cache.put(template, r);
    }
    return r;
  }
  /**
   * Get a list of variables embedded in the template string.
   */
  public List<String> getVariables(String template) {
    Resolved r = getResolved(template);
    if (r == null) {
      return Collections.emptyList();
    }
    return new ArrayList<>(r.variables);
  }
  
  public void addNamespace(String name, Map<String,Object> newMap) {
    if (newMap != null) {
      if (name != null) {
        String[] nameParts = DOT_PATTERN.split(name);
        Map<String,Object> nameResolveLevel = currentLevelMap(nameParts,
            rootNamespace, false).map;
        nameResolveLevel.put(nameParts[nameParts.length - 1], newMap);
      } else {
        for (Map.Entry<String,Object> entry : newMap.entrySet()) {
          String[] keyParts = DOT_PATTERN.split(entry.getKey());
          Map<String,Object> currentLevel = rootNamespace;
          currentLevel = currentLevelMap(keyParts, currentLevel, false).map;
          currentLevel.put(keyParts[keyParts.length - 1], entry.getValue());
        }
      }
    }
  }
  
  class CurrentLevel {
    final Map<String,Object> map;
    final int level;
    CurrentLevel(int level, Map<String,Object> map) {
      this.level = level;
      this.map = map;
    }   
  }
  
  private CurrentLevel currentLevelMap(String[] keyParts,
      Map<String,Object> currentLevel, boolean includeLastLevel) {
    int j = includeLastLevel ? keyParts.length : keyParts.length - 1;
    for (int i = 0; i < j; i++) {
      Object o = currentLevel.get(keyParts[i]);
      if (o == null) {
        if(i == j-1) {
          Map<String,Object> nextLevel = new HashMap<>();
          currentLevel.put(keyParts[i], nextLevel);
          currentLevel = nextLevel;
        } else {
          return new CurrentLevel(i, currentLevel);
        }
      } else if (o instanceof Map<?,?>) {
        @SuppressWarnings("unchecked")
        Map<String,Object> nextLevel = (Map<String,Object>) o;
        currentLevel = nextLevel;
      } else {
        throw new AssertionError(
            "Non-leaf nodes should be of type java.util.Map");
      }
    }
    return new CurrentLevel(j-1, currentLevel);
  }
  
  public void removeNamespace(String name) {
    rootNamespace.remove(name);
  }
  
  public void setEvaluators(Map<String,Evaluator> evaluators) {
    this.evaluators = evaluators;
  }
}
