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
import org.apache.solr.search.facet.FacetFieldProcessorByArrayDV.SegCountGlobal;
import org.apache.solr.search.facet.FacetFieldProcessorByArrayDV.SegCountPerSeg;

// nocommit: need class & method javadocs explaining purpose/usage
interface SweepCountAware {

  boolean collectBase();

  // nocommit: if SegCountGlobal & SegCountPerSeg both extend SegCounter,
  // nocommit: and if SegCounter defines the common 'map' method,
  // nocommit: and if 'map' is the only method the impls of 'registerCounts' (may) use,
  // nocommit: then: why do we need a unique API/impl of registerCounts for each subclass of SegCounter?

  
  int registerCounts(SegCountGlobal segCounts) throws IOException;

  int registerCounts(SegCountPerSeg segCounts) throws IOException;

}
