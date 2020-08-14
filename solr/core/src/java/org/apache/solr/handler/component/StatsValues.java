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
package org.apache.solr.handler.component;


import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;

/**
 * StatsValue defines the interface for the collection of statistical values about fields and facets.
 */
// TODO: should implement Collector?
public interface StatsValues {

  /**
   * Accumulate the values based on those in the given NamedList
   *
   * @param stv NamedList whose values will be used to accumulate the current values
   */
  void accumulate(@SuppressWarnings({"rawtypes"})NamedList stv);

  /** Accumulate the value associated with <code>docID</code>.
   *  @see #setNextReader(org.apache.lucene.index.LeafReaderContext) */
  void accumulate(int docID) throws IOException;

  /**
   * Accumulate the values based on the given value
   *
   * @param value Value to use to accumulate the current values
   * @param count number of times to accumulate this value
   */
  void accumulate(BytesRef value, int count);

  /**
   * Updates the statistics when a document is missing a value
   */
  void missing();

  /**
   * Updates the statistics when multiple documents are missing a value
   *
   * @param count number of times to count a missing value
   */
  void addMissing(int count);

   /**
   * Adds the facet statistics for the facet with the given name
   *
   * @param facetName Name of the facet
   * @param facetValues Facet statistics on a per facet value basis
   */
  void addFacet(String facetName, Map<String, StatsValues> facetValues);

  /**
   * Translates the values into a NamedList representation
   *
   * @return NamedList representation of the current values
   */
  NamedList<?> getStatsValues();

  /** Set the context for {@link #accumulate(int)}. */
  void setNextReader(LeafReaderContext ctx) throws IOException;
}
