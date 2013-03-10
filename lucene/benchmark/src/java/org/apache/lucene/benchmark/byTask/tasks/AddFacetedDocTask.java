package org.apache.lucene.benchmark.byTask.tasks;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.FacetSource;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.taxonomy.CategoryPath;

/**
 * Add a faceted document.
 * <p>
 * Config properties:
 * <ul>
 * <li><b>with.facets</b>=&lt;tells whether to actually add any facets to the
 * document| Default: true&gt; <br>
 * This config property allows to easily compare the performance of adding docs
 * with and without facets. Note that facets are created even when this is
 * false, just that they are not added to the document (nor to the taxonomy).
 * </ul>
 * <p>
 * See {@link AddDocTask} for general document parameters and configuration.
 * <p>
 * Makes use of the {@link FacetSource} in effect - see {@link PerfRunData} for
 * facet source settings.
 */
public class AddFacetedDocTask extends AddDocTask {

  private final List<CategoryPath> facets = new ArrayList<CategoryPath>();
  private FacetFields facetFields;
  
  public AddFacetedDocTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    if (facetFields == null) {
      boolean withFacets = getRunData().getConfig().get("with.facets", true);
      if (withFacets) {
        FacetSource facetsSource = getRunData().getFacetSource();
        facetFields = withFacets ? new FacetFields(getRunData().getTaxonomyWriter()) : null;
        facetsSource.getNextFacets(facets);
      }
    }
  }

  @Override
  protected String getLogMessage(int recsCount) {
    if (facetFields == null) {
      return super.getLogMessage(recsCount);
    }
    return super.getLogMessage(recsCount)+ " with facets";
  }
  
  @Override
  public int doLogic() throws Exception {
    if (facetFields != null) {
      facetFields.addFields(doc, facets);
    }
    return super.doLogic();
  }

}
