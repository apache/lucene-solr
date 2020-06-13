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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;

public class ObjectUtil {

  public static class ConflictHandler {
    protected boolean isList(Map<String,Object> container, List<String> path, String key, Object current, Object previous) {
      return key!=null && ("fields".equals(key) || "filter".equals(key));
    }

    public void handleConflict(Map<String,Object> container, List<String> path, String key, Object current, Object previous) {
      boolean handleAsList = isList(container, path, key, current, previous);
      if (handleAsList) {
        container.put(key, makeList(current, previous) );
        return;
      }

      if (previous instanceof Map && current instanceof Map) {
        @SuppressWarnings({"unchecked"})
        Map<String,Object> prevMap = (Map<String,Object>)previous;
        @SuppressWarnings({"unchecked"})
        Map<String,Object> currMap = (Map<String,Object>)current;
        if (prevMap.size() == 0) return;
        mergeMap(prevMap, currMap, path);
        container.put(key, prevMap);
        return;
      }

      // if we aren't handling as a list, and we aren't handling as a map, then just overwrite (i.e. nothing else to do)
      return;
    }


    // merges srcMap onto targetMap (i.e. changes targetMap but not srcMap)
    public void mergeMap(Map<String,Object> targetMap, Map<String,Object> srcMap, List<String> path) {
      if (srcMap.size() == 0) return;
      // to keep ordering correct, start with prevMap and merge in currMap
      for (Map.Entry<String,Object> srcEntry : srcMap.entrySet()) {
        String subKey = srcEntry.getKey();
        Object subVal = srcEntry.getValue();
        Object subPrev = targetMap.put(subKey, subVal);
        if (subPrev != null) {
          // recurse
          path.add(subKey);
          handleConflict(targetMap, path, subKey, subVal, subPrev);
          path.remove(path.size()-1);
        }
      }
    }

    protected Object makeList(Object current, Object previous) {
      @SuppressWarnings({"rawtypes"})
      ArrayList lst = new ArrayList();
      append(lst, previous);   // make the original value(s) come first
      append(lst, current);
      return lst;
    }

    @SuppressWarnings({"unchecked"})
    protected void append(@SuppressWarnings({"rawtypes"})List lst, Object current) {
      if (current instanceof Collection) {
        lst.addAll((Collection)current);
      } else {
        lst.add(current);
      }
    }

  }

  public static void mergeObjects(Map<String,Object> top, List<String> path, Object val, ConflictHandler handler) {
    Map<String,Object> outer = top;
    for (int i=0; i<path.size()-1; i++) {
      @SuppressWarnings({"unchecked"})
      Map<String,Object> sub = (Map<String,Object>)outer.get(path.get(i));
      if (sub == null) {
        sub = new LinkedHashMap<String,Object>();
        outer.put(path.get(i), sub);
      }
      outer = sub;
    }

    String key = path.size() > 0 ? path.get(path.size()-1) : null;

    if (key != null) {
      Object existingVal = outer.put(key, val);
      if (existingVal != null) {
        // OK, now we need to merge values
        handler.handleConflict(outer, path, key, val, existingVal);
      }
    } else if (val instanceof Map) {
      // merging at top level...
      @SuppressWarnings({"unchecked"})
      Map<String,Object> newMap = (Map<String,Object>)val;
      handler.mergeMap(outer, newMap, path);
    } else {
      // todo: find a way to return query param in error message
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expected JSON Object but got " + val.getClass().getSimpleName() + "=" + val);
    }
  }

}
