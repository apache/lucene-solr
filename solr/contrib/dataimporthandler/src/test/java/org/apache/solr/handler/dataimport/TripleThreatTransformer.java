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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This transformer does 3 things
 * <ul>
 * <li>It turns every row into 3 rows, 
 *     modifying any "id" column to ensure duplicate entries in the index
 * <li>The 2nd Row has 2x values for every column, 
 *   with the added one being backwards of the original
 * <li>The 3rd Row has an added static value
 * </ul>
 * 
 * Also, this does not extend Transformer.
 */
public class TripleThreatTransformer {
  public Object transformRow(Map<String, Object> row) {
    List<Map<String, Object>> rows = new ArrayList<>(3);
    rows.add(row);
    rows.add(addDuplicateBackwardsValues(row));
    rows.add(new LinkedHashMap<>(row));
    rows.get(2).put("AddAColumn_s", "Added");
    modifyIdColumn(rows.get(1), 1);
    modifyIdColumn(rows.get(2), 2);
    return rows;
  }
  private LinkedHashMap<String,Object> addDuplicateBackwardsValues(Map<String, Object> row) {
    LinkedHashMap<String,Object> n = new LinkedHashMap<>();
    for(Map.Entry<String,Object> entry : row.entrySet()) {
      String key = entry.getKey();
      if(!"id".equalsIgnoreCase(key)) {
        String[] vals = new String[2];
        vals[0] = entry.getValue()==null ? "null" : entry.getValue().toString();
        vals[1] = new StringBuilder(vals[0]).reverse().toString();
        n.put(key, Arrays.asList(vals));
      } else {
        n.put(key, entry.getValue());
      }
    }
    return n;
  }
  
  private void modifyIdColumn(Map<String, Object> row, int num) {
    Object o = row.remove("ID");
    if(o==null) {
      o = row.remove("id");
    }
    if(o!=null) {
      String id = o.toString();
      id = "TripleThreat-" + num + "-" + id;
      row.put("id", id);
    }
  }
}
