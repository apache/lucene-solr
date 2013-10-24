package org.apache.lucene.benchmark.byTask.feeds;

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

import org.apache.lucene.facet.taxonomy.CategoryPath;

/**
 * Source items for facets.
 * <p>
 * For supported configuration parameters see {@link ContentItemsSource}.
 */
public abstract class FacetSource extends ContentItemsSource {

  /**
   * Fills the next facets content items in the given list. Implementations must
   * account for multi-threading, as multiple threads can call this method
   * simultaneously.
   */
  public abstract void getNextFacets(List<CategoryPath> facets) throws NoMoreDataException, IOException;

  @Override
  public void resetInputs() throws IOException {
    printStatistics("facets");
    // re-initiate since properties by round may have changed.
    setConfig(getConfig());
    super.resetInputs();
  }
  
}
