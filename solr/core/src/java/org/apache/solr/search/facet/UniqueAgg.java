package org.apache.solr.search.facet;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;

public class UniqueAgg extends StrAggValueSource {
  public static String UNIQUE = "unique";

  // internal constants used for aggregating values from multiple shards
  static String VALS = "vals";

  public UniqueAgg(String field) {
    super(UNIQUE, field);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(getArg());
    if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
      if (sf.hasDocValues()) {
        return new UniqueMultiDvSlotAcc(fcontext, getArg(), numSlots);
      } else {
        return new UniqueMultivaluedSlotAcc(fcontext, getArg(), numSlots);
      }
    } else {
      return new UniqueSinglevaluedSlotAcc(fcontext, getArg(), numSlots);
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetMerger() {
      long sumUnique;
      Set<Object> values;
      int shardsMissing;
      long shardsMissingSum;
      long shardsMissingMax;

      @Override
      public void merge(Object facetResult) {
        SimpleOrderedMap map = (SimpleOrderedMap)facetResult;
        long unique = ((Number)map.get("unique")).longValue();
        sumUnique += unique;

        List vals = (List)map.get("vals");
        if (vals != null) {
          if (values == null) {
            values = new HashSet<>(vals.size()*4);
          }
          values.addAll(vals);
        } else {
          shardsMissing++;
          shardsMissingSum += unique;
          shardsMissingMax = Math.max(shardsMissingMax, unique);
        }

        // TODO: somehow get & use the count in the bucket?
      }

      @Override
      public Object getMergedResult() {
        long exactCount = values == null ? 0 : values.size();
        return exactCount + shardsMissingSum;
      }
    };
  }
}
