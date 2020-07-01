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

package org.apache.solr.request.json;

import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;

/**
 * Convert json query object to local params.
 *
 * @lucene.internal
 */
class JsonQueryConverter {
  private int numParams = 0;

  String toLocalParams(Object jsonQueryObject, Map<String, String[]> additionalParams) {
    if (jsonQueryObject instanceof String) return jsonQueryObject.toString();
    StringBuilder builder = new StringBuilder();
    buildLocalParams(builder, jsonQueryObject, true, additionalParams);
    return builder.toString();
  }

  private String putParam(String val, Map<String, String[]> additionalParams) {
    String name = "_tt"+(numParams++);
    additionalParams.put(name, new String[]{val});
    return name;
  }

  // when isQParser==true, "val" is a query object of the form {query_type:{param1:val1, param2:val2}}
  // when isQParser==false, "val" is a parameter on an existing qparser (which could be a simple parameter like 42, or a sub-query)
  @SuppressWarnings({"unchecked"})
  private void buildLocalParams(StringBuilder builder, Object val, boolean isQParser, Map<String, String[]> additionalParams) {
    if (!isQParser && !(val instanceof Map)) {
      // val is value of a query parser, and it is not a map
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Error when parsing json query, expect a json object here, but found : "+val);
      // NOTE: a top-level query *can* be a String, so we should really allow it here.  This currently only works because
      // we special-case String in toLocalParams() and don't call this method.
    }
    // We don't want to introduce unnecessary variable at root level
    boolean useSubBuilder = builder.length() > 0;

    if (val instanceof String) {
      if (!useSubBuilder) {
        // Top level, so just use the value.  NOTE: this case is also short-circuited in toLocalParams() for performance.
        builder.append(val.toString());
      } else {
        // val is a parameter in a qparser, so use param deref and skip escaping: ...=$param1}&param1=<val>
        builder.append('$').append(putParam(val.toString(), additionalParams));
      }
      return;
    }
    if (val instanceof Number) {
      builder.append(val);
      return;
    }
    if (!(val instanceof Map)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Error when parsing json query, expect a json object here, but found : "+val);
    }

    Map<String,Object> map = (Map<String, Object>) val;
    if (isQParser) {
      if (map.size() != 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Error when parsing json query, expect only one query parser here, but found : "+map.keySet());
      }

      String qtype = map.keySet().iterator().next();
      String tagName = null;
      if (qtype.startsWith("#")) {
        Object taggedQueryObject = map.get(qtype);
        tagName = qtype.substring(1);
        if (taggedQueryObject instanceof String) {
          StringBuilder sb = new StringBuilder();
          sb.append("{!tag=").append(tagName).append("}");
          sb.append(taggedQueryObject.toString());
          buildLocalParams(builder, sb.toString(), true, additionalParams);
          return;
        } else if (taggedQueryObject instanceof Map) {
          map = (Map<String, Object>) taggedQueryObject;
          qtype = map.keySet().iterator().next();
          // FUTURE: might want to recurse here instead to handle nested tags (and add tagName as a parameter?)
        }
      } else {
        if (qtype.equals("param")) {
          boolean toplevel;
          if (toplevel=(builder.length() == 0)) {
            builder.append("{!v=");  
          }
          builder.append("$").append(map.get("param"));
          if (toplevel) {
            builder.append("}");
          }
          return;
        }
      }

      StringBuilder subBuilder = useSubBuilder ? new StringBuilder() : builder;

      Object subVal = map.get(qtype);
      subBuilder = subBuilder.append("{!").append(qtype).append(' ');
      if (tagName != null) {
        subBuilder.append("tag=").append(tagName).append(' ');
      }
      buildLocalParams(subBuilder, subVal, false, additionalParams);
      subBuilder.append("}");

      if (useSubBuilder) {
        builder.append('$').append(putParam(subBuilder.toString(), additionalParams));
      }
    } else {
      if(map.size()==1 && map.keySet().iterator().next().equals("param")) {
        builder.append("v").append("=$").append(map.get("param")).append(" ");
      } else {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          String key = entry.getKey();
          if (entry.getValue() instanceof List) {
            if (key.equals("query")) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "Error when parsing json query, value of query field should not be a list, found : " + entry.getValue());
            }
            @SuppressWarnings({"rawtypes"})
            List l = (List) entry.getValue();
            for (Object subVal : l) {
              builder.append(key).append("=");
              buildLocalParams(builder, subVal, true, additionalParams);
              builder.append(" ");
            }
          } else {
            if (key.equals("query")) {
              key = "v";
            }
            builder.append(key).append("=");
            buildLocalParams(builder, entry.getValue(), true, additionalParams);
            builder.append(" ");
          }
        }
      }
    }
  }
}
