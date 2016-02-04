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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SortedMapBackedCache implements DIHCache {
  private SortedMap<Object,List<Map<String,Object>>> theMap = null;
  private boolean isOpen = false;
  private boolean isReadOnly = false;
  String primaryKeyName = null;
  
  @SuppressWarnings("unchecked")
  @Override
  public void add(Map<String,Object> rec) {
    checkOpen(true);
    checkReadOnly();
    
    if (rec == null || rec.size() == 0) {
      return;
    }
    
    if (primaryKeyName == null) {
      primaryKeyName = rec.keySet().iterator().next();
    }
    
    Object pk = rec.get(primaryKeyName);
    if (pk instanceof Collection<?>) {
      Collection<Object> c = (Collection<Object>) pk;
      if (c.size() != 1) {
        throw new RuntimeException(
            "The primary key must have exactly 1 element.");
      }
      pk = c.iterator().next();
    }
    //Rows with null keys are not added.
    if(pk==null) {
      return;
    }
    List<Map<String,Object>> thisKeysRecs = theMap.get(pk);
    if (thisKeysRecs == null) {
      thisKeysRecs = new ArrayList<>();
      theMap.put(pk, thisKeysRecs);
    }
    thisKeysRecs.add(rec);
  }
  
  private void checkOpen(boolean shouldItBe) {
    if (!isOpen && shouldItBe) {
      throw new IllegalStateException(
          "Must call open() before using this cache.");
    }
    if (isOpen && !shouldItBe) {
      throw new IllegalStateException("The cache is already open.");
    }
  }
  
  private void checkReadOnly() {
    if (isReadOnly) {
      throw new IllegalStateException("Cache is read-only.");
    }
  }
  
  @Override
  public void close() {
    isOpen = false;
  }
  
  @Override
  public void delete(Object key) {
    checkOpen(true);
    checkReadOnly();
    if(key==null) {
      return;
    }
    theMap.remove(key);
  }
  
  @Override
  public void deleteAll() {
    deleteAll(false);
  }
  
  private void deleteAll(boolean readOnlyOk) {
    if (!readOnlyOk) {
      checkReadOnly();
    }
    if (theMap != null) {
      theMap.clear();
    }
  }
  
  @Override
  public void destroy() {
    deleteAll(true);
    theMap = null;
    isOpen = false;
  }
  
  @Override
  public void flush() {
    checkOpen(true);
    checkReadOnly();
  }
  
  @Override
  public Iterator<Map<String,Object>> iterator(Object key) {
    checkOpen(true);
    if(key==null) {
      return null;
    }
    if(key instanceof Iterable<?>) {
      List<Map<String,Object>> vals = new ArrayList<>();
      Iterator<?> iter = ((Iterable<?>) key).iterator();
      while(iter.hasNext()) {
        List<Map<String,Object>> val = theMap.get(iter.next());
        if(val!=null) {
          vals.addAll(val);
        }
      } 
      if(vals.size()==0) {
        return null;
      }
      return vals.iterator();
    }    
    List<Map<String,Object>> val = theMap.get(key);
    if (val == null) {
      return null;
    }
    return val.iterator();
  }
  
  @Override
  public Iterator<Map<String,Object>> iterator() {
    return new Iterator<Map<String, Object>>() {
        private Iterator<Map.Entry<Object,List<Map<String,Object>>>> theMapIter;
        private List<Map<String,Object>> currentKeyResult = null;
        private Iterator<Map<String,Object>> currentKeyResultIter = null;

        {
            theMapIter = theMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
          if (currentKeyResultIter != null) {
            if (currentKeyResultIter.hasNext()) {
              return true;
            } else {
              currentKeyResult = null;
              currentKeyResultIter = null;
            }
          }

          Map.Entry<Object,List<Map<String,Object>>> next = null;
          if (theMapIter.hasNext()) {
            next = theMapIter.next();
            currentKeyResult = next.getValue();
            currentKeyResultIter = currentKeyResult.iterator();
            if (currentKeyResultIter.hasNext()) {
              return true;
            }
          }
          return false;
        }

        @Override
        public Map<String,Object> next() {
          if (currentKeyResultIter != null) {
            if (currentKeyResultIter.hasNext()) {
              return currentKeyResultIter.next();
            } else {
              currentKeyResult = null;
              currentKeyResultIter = null;
            }
          }

          Map.Entry<Object,List<Map<String,Object>>> next = null;
          if (theMapIter.hasNext()) {
            next = theMapIter.next();
            currentKeyResult = next.getValue();
            currentKeyResultIter = currentKeyResult.iterator();
            if (currentKeyResultIter.hasNext()) {
              return currentKeyResultIter.next();
            }
          }
          return null;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
    };
  }

    @Override
  public void open(Context context) {
    checkOpen(false);
    isOpen = true;
    if (theMap == null) {
      theMap = new TreeMap<>();
    }
    
    String pkName = CachePropertyUtil.getAttributeValueAsString(context,
        DIHCacheSupport.CACHE_PRIMARY_KEY);
    if (pkName != null) {
      primaryKeyName = pkName;
    }
    isReadOnly = false;
    String readOnlyStr = CachePropertyUtil.getAttributeValueAsString(context,
        DIHCacheSupport.CACHE_READ_ONLY);
    if ("true".equalsIgnoreCase(readOnlyStr)) {
      isReadOnly = true;
    }
  }
  
}
