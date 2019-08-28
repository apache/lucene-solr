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

package runtimecode;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.solr.search.LRUCache;

public  class MyDocCache<K,V> extends LRUCache<K,V> {

  static String fld_name= "my_synthetic_fld_s";
  @Override
  public V put(K key, V value) {
    if(value instanceof Document){
      Document d = (Document) value;
      d.add(new StoredField(fld_name, "version_2"));
    }
    return super.put(key, value);
  }
}
