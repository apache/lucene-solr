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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

public class Preference implements MapWriter {
  final Policy.SortParam name;
  Integer precision;
  final Policy.Sort sort;
  Preference next;
  public int idx;
  private final Map original;

  Preference(Map<String, Object> m) {
    this.original = Utils.getDeepCopy(m,3);
    sort = Policy.Sort.get(m);
    name = Policy.SortParam.get(m.get(sort.name()).toString());
    Object p = m.getOrDefault("precision", 0);
    precision = p instanceof Number ? ((Number) p).intValue() : Integer.parseInt(p.toString());
    if (precision < 0) {
      throw new RuntimeException("precision must be a positive value ");
    }
    if(precision< name.min || precision> name.max){
      throw new RuntimeException(StrUtils.formatString("invalid precision value {0} must lie between {1} and {1}",
          precision, name.min, name.max ) );
    }

  }

  // there are 2 modes of compare.
  // recursive, it uses the precision to tie & when there is a tie use the next preference to compare
  // in non-recursive mode, precision is not taken into consideration and sort is done on actual value
  int compare(Row r1, Row r2, boolean useApprox) {
    Object o1 = useApprox ? r1.cells[idx].approxVal : r1.cells[idx].val;
    Object o2 = useApprox ? r2.cells[idx].approxVal : r2.cells[idx].val;
    int result = 0;
    if (o1 instanceof Long && o2 instanceof Long) result = ((Long) o1).compareTo((Long) o2);
    else if (o1 instanceof Double && o2 instanceof Double) result = ((Double) o1).compareTo((Double) o2);
    else if (!o1.getClass().getName().equals(o2.getClass().getName()))  {
      throw new RuntimeException("Unable to compare " + o1 + " of type: " + o1.getClass().getName() + " from " + r1.cells[idx].toString() + " and " + o2 + " of type: " + o2.getClass().getName() + " from " + r2.cells[idx].toString());
    }
    return result == 0 ? (next == null ? 0 : next.compare(r1, r2, useApprox)) : sort.sortval * result;
  }

  //sets the new value according to precision in val_
  void setApproxVal(List<Row> tmpMatrix) {
    Object prevVal = null;
    for (Row row : tmpMatrix) {
      prevVal = row.cells[idx].approxVal =
          (prevVal == null || Double.compare(Math.abs(((Number) prevVal).doubleValue() - ((Number) row.cells[idx].val).doubleValue()), precision) > 0) ?
              row.cells[idx].val :
              prevVal;
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    for (Object o : original.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      ew.put(String.valueOf(e.getKey()), e.getValue());
    }
  }

  public Policy.SortParam getName() {
    return name;
  }
}
