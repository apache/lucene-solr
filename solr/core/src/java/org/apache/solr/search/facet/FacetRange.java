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
package org.apache.solr.search.facet;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;

public class FacetRange extends FacetRequestSorted {
  static final String ACTUAL_END_JSON_KEY = "_actual_end";
  String field;
  Object start;
  Object end;
  Object gap;
  Object ranges;
  boolean hardend = false;
  EnumSet<FacetRangeInclude> include;
  EnumSet<FacetRangeOther> others;

  {
    // defaults
    mincount = 0;
    limit = -1;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    return new FacetRangeProcessor(fcontext, this);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetRangeMerger(this);
  }

  @Override
  public Map<String, Object> getFacetDescription() {
    Map<String, Object> descr = new HashMap<>();
    descr.put("field", field);
    if (ranges != null) {
      descr.put("ranges", ranges);
    } else {
      descr.put("start", start);
      descr.put("end", end);
      descr.put("gap", gap);
    }
    return descr;
  }
}
