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

package org.apache.solr.client.solrj.request;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Template;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;

import static org.apache.solr.common.util.ValidatingJsonMap.NOT_NULL;


public class V1toV2ApiMapper {

  private static EnumMap<CollectionAction, ActionInfo> mapping = new EnumMap<>(CollectionAction.class);

  static {
    for (CollectionApiMapping.Meta meta : CollectionApiMapping.Meta.values()) {
      if (meta.action != null) mapping.put(meta.action, new ActionInfo(meta));
    }
  }

  private static class ActionInfo {
    CollectionApiMapping.Meta meta;
    String path;
    Template template;


    JsonSchemaValidator validator;

    ActionInfo(CollectionApiMapping.Meta meta) {
      this.meta = meta;
    }

    //do this lazily because , it makes no sense if this is not used
    synchronized void setPath() {
      if (path == null) {
        ValidatingJsonMap m = Utils.getSpec(meta.getEndPoint().getSpecName()).getSpec();
        Object o = Utils.getObjectByPath(m, false, "url/paths");

        String result = null;
        if (o instanceof List) {//choose the shortest path
          for (Object s : (List) o) {
            if (result == null || s.toString().length() < result.length()) result = s.toString();
          }
        } else if (o instanceof String) {
          result = (String) o;
        }
        path = result;
        template = new Template(path, Template.BRACES_PLACEHOLDER_PATTERN);

        validator = new JsonSchemaValidator(m.getMap("commands", NOT_NULL).getMap(meta.commandName, NOT_NULL));
      }
    }

    public V2Request.Builder convert(SolrParams paramsV1) {
      String[] list = new String[template.variables.size()];
      MapWriter data = serializeToV2Format(paramsV1, list);
      @SuppressWarnings({"rawtypes"})
      Map o = data.toMap(new LinkedHashMap<>());
      return new V2Request.Builder(template.apply(s -> {
        int idx = template.variables.indexOf(s);
        return list[idx];
      }))
          .withMethod(meta.getHttpMethod())
          .withPayload(o);

    }

    private MapWriter serializeToV2Format(SolrParams paramsV1, String[] list) {
      return ew -> ew.put(meta.commandName, (MapWriter) ew1 -> {
        Iterator<String> iter = paramsV1.getParameterNamesIterator();
        Map<String, Map<String, String>> subProperties = null;
        while (iter.hasNext()) {
          String key = iter.next();
          if (CoreAdminParams.ACTION.equals(key)) continue;
          Object substitute = meta.getReverseParamSubstitute(key);
          int idx = template.variables.indexOf(substitute);
          if (idx > -1) {
            String val = paramsV1.get(key);
            if (val == null) throw new RuntimeException("null value is not valid for " + key);
            list[idx] = val;
            continue;
          }
          if (substitute instanceof Pair) {//this is a nested object
            @SuppressWarnings("unchecked")
            Pair<String, String> p = (Pair<String, String>) substitute;
            if (subProperties == null) subProperties = new HashMap<>();
            subProperties.computeIfAbsent(p.first(), s -> new HashMap<>()).put(p.second(), paramsV1.get(key));
          } else {
            Object val = paramsV1.get(key);
            ew1.put(substitute.toString(), val);
          }
        }
        if (subProperties != null) {
          for (Map.Entry<String, Map<String, String>> e : subProperties.entrySet()) {
            ew1.put(e.getKey(), e.getValue());
          }
        }
      });
    }
  }


  public static V2Request.Builder convert(CollectionAdminRequest<?> request) {
    ActionInfo info = mapping.get(request.action);
    if (info == null) throw new RuntimeException("Unsupported action :" + request.action);

    if (info.meta.getHttpMethod() == SolrRequest.METHOD.POST) {
      if (info.path == null) info.setPath();
      return info.convert(request.getParams());
    }

    return null;
  }


}
