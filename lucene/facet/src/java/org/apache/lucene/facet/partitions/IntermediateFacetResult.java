package org.apache.lucene.facet.partitions;

import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultsHandler;

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

/**
 * Intermediate {@link FacetResult} of faceted search.
 * <p>
 * This is an empty interface on purpose.
 * <p>
 * It allows {@link FacetResultsHandler} to return intermediate result objects 
 * that only it knows how to interpret, and so the handler has maximal freedom
 * in defining what an intermediate result is, depending on its specific logic.  
 * 
 * @lucene.experimental
 */
public interface IntermediateFacetResult {

  /**
   * Facet request for which this temporary result was created.
   */
  FacetRequest getFacetRequest();

}
