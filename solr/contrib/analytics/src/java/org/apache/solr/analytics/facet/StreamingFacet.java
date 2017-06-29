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
package org.apache.solr.analytics.facet;

import org.apache.solr.analytics.AnalyticsDriver;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;

/**
 * A facet that is collected during the streaming phase of the {@link AnalyticsDriver}.
 */
public interface StreamingFacet {
  /**
   * Determine which facet values match the current document. Add the {@link ReductionDataCollection}s of the relevant facet values
   * to the targets of the streaming {@link ReductionCollectionManager} so that they are updated with the current document's data.
   */
  void addFacetValueCollectionTargets();
}
