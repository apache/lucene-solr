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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.SimpleOrderedMap;

public class FacetQuery extends FacetRequest {
  // query string or query?
  Query q;

  @SuppressWarnings("rawtypes")
  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    return new FacetQueryProcessor(fcontext, this);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetModule.FacetQueryMerger(this);
  }
  
  @Override
  public Map<String, Object> getFacetDescription() {
    Map<String, Object> descr = new HashMap<String, Object>();
    descr.put("query", q);
    return descr;
  }
}




class FacetQueryProcessor extends FacetProcessor<FacetQuery> {
  FacetQueryProcessor(FacetContext fcontext, FacetQuery freq) {
    super(fcontext, freq);
  }

  @Override
  public void process() throws IOException {
    super.process();

    if (fcontext.facetInfo != null) {
      // FIXME - what needs to be done here?
    }
    response = new SimpleOrderedMap<>();
    fillBucket(response, freq.q, null, (fcontext.flags & FacetContext.SKIP_FACET)!=0, fcontext.facetInfo);
  }


}

