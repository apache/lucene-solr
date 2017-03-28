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

package org.apache.solr.recipe;

import java.util.List;
import java.util.Map;

class Preference {
  final RuleSorter.SortParam name;
  Integer precision;
  final RuleSorter.Sort sort;
  Preference next;
  public int idx;

  Preference(Map<String, Object> m) {
    sort = RuleSorter.Sort.get(m);
    name = RuleSorter.SortParam.get(m.get(sort.name()).toString());
    Object p = m.getOrDefault("precision", 0);
    precision = p instanceof Number ? ((Number) p).intValue() : Integer.parseInt(p.toString());

  }

  // there are 2 modes of compare.
  // recursive, it uses the precision to tie & when there is a tie use the next preference to compare
  // in non-recursive mode, precision is not taken into consideration and sort is done on actual value
  int compare(Row r1, Row r2, boolean recursive) {
    Object o1 = recursive ? r1.cells[idx].val_ : r1.cells[idx].val;
    Object o2 = recursive ? r2.cells[idx].val_ : r2.cells[idx].val;
    int result = 0;
    if (o1 instanceof Integer && o2 instanceof Integer) result = ((Integer) o1).compareTo((Integer) o2);
    if (o1 instanceof Long && o2 instanceof Long) result = ((Long) o1).compareTo((Long) o2);
    if (o1 instanceof Float && o2 instanceof Float) result = ((Float) o1).compareTo((Float) o2);
    if (o1 instanceof Double && o2 instanceof Double) result = ((Double) o1).compareTo((Double) o2);
    return result == 0 ? next == null ? 0 : next.compare(r1, r2, recursive) : sort.sortval * result;
  }

  //sets the new value according to precision in val_
  void setApproxVal(List<Row> tmpMatrix) {
    Object prevVal = null;
    for (Row row : tmpMatrix) {
      prevVal = row.cells[idx].val_ =
          prevVal == null || Math.abs(((Number) prevVal).longValue() - ((Number) row.cells[idx].val).longValue()) > precision ?
              row.cells[idx].val :
              prevVal;
    }
  }
}
