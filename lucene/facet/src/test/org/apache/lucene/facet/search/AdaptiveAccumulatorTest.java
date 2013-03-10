package org.apache.lucene.facet.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.LuceneTestCase.Slow;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.sampling.BaseSampleTestTopK;
import org.apache.lucene.facet.sampling.Sampler;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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

@Slow
public class AdaptiveAccumulatorTest extends BaseSampleTestTopK {

  @Override
  protected StandardFacetsAccumulator getSamplingAccumulator(Sampler sampler, TaxonomyReader taxoReader, 
      IndexReader indexReader, FacetSearchParams searchParams) {
    AdaptiveFacetsAccumulator res = new AdaptiveFacetsAccumulator(searchParams, indexReader, taxoReader);
    res.setSampler(sampler);
    return res;
  }
  
}
