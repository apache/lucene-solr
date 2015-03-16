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
import java.util.List;

import org.apache.lucene.queries.function.ValueSource;


public class AvgAgg extends SimpleAggValueSource {
  public AvgAgg(ValueSource vs) {
    super("avg", vs);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    return new AvgSlotAcc(getArg(), fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetMerger() {
      long num;
      double sum;

      @Override
      public void merge(Object facetResult) {
        List<Number> numberList = (List<Number>)facetResult;
        num += numberList.get(0).longValue();
        sum += numberList.get(1).doubleValue();
      }

      @Override
      public Object getMergedResult() {
        return num==0 ? 0.0d : sum/num;
      }
    };
  }
}
