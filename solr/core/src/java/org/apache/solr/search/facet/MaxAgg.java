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

import org.apache.lucene.queries.function.ValueSource;


public class MaxAgg extends SimpleAggValueSource {
  public MaxAgg(ValueSource vs) {
    super("max", vs);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    return new MaxSlotAcc(getArg(), fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetMerger() {
      double val = Double.NaN;

      @Override
      public void merge(Object facetResult) {
        double result = ((Number)facetResult).doubleValue();
        if (result > val || Double.isNaN(val)) {
          val = result;
        }
      }

      @Override
      public Object getMergedResult() {
        return val;
      }
    };
  }


}
