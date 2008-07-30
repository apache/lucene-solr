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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * <p>
 * The default implementation of VariableResolver interface
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @see VariableResolver
 * @since solr 1.3
 */
public class VariableResolverImpl extends VariableResolver {
  private Map<String, Object> container = new HashMap<String, Object>();

  private static final TemplateString TEMPLATE_STRING = new TemplateString();

  public VariableResolverImpl() {
  }

  @SuppressWarnings("unchecked")
  public VariableResolverImpl addNamespace(String name, Map<String, Object> map) {
    if (name != null) {
      String[] parts = DOT_SPLIT.split(name, 0);
      Map ns = container;
      for (int i = 0; i < parts.length; i++) {
        if (i == parts.length - 1) {
          ns.put(parts[i], map);
        }
        if (ns.get(parts[i]) == null) {
          ns.put(parts[i], new HashMap());
          ns = (Map) ns.get(parts[i]);
        } else {
          if (ns.get(parts[i]) instanceof Map) {
            ns = (Map) ns.get(parts[i]);
          } else {
            ns.put(parts[i], new HashMap());
            ns = (Map) ns.get(parts[i]);
          }
        }
      }

    } else {
      container.putAll(map);
    }
    return this;

  }

  public void removeNamespace(String name) {
    if (name != null)
      container.remove(name);
  }

  public String replaceTokens(String template) {
    return TEMPLATE_STRING.replaceTokens(template, this);
  }

  @SuppressWarnings("unchecked")
  public Object resolve(String name) {
    if (name == null)
      return container;
    if ("".equals(name))
      return null;
    String[] parts = DOT_SPLIT.split(name, 0);
    Map<String, Object> namespace = container;
    for (int i = 0; i < parts.length; i++) {
      String thePart = parts[i];
      if (i == parts.length - 1) {
        return namespace.get(thePart);
      }
      Object temp = namespace.get(thePart);
      if (temp == null) {
        return namespace.get(mergeAll(parts, i));
      } else {
        if (temp instanceof Map) {
          namespace = (Map) temp;
        } else {
          return null;
        }
      }
    }
    return null;
  }

  private String mergeAll(String[] parts, int i) {
    if (i == parts.length - 1)
      return parts[parts.length - 1];
    StringBuffer sb = new StringBuffer();
    for (int j = i; j < parts.length; j++) {
      sb.append(parts[j]);
      if (j < parts.length - 1)
        sb.append(".");
    }
    return sb.toString();
  }

  static final Pattern DOT_SPLIT = Pattern.compile("\\.");
}
