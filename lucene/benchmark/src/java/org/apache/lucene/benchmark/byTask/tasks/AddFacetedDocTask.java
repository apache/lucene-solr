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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.IndexDocument;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StorableField;

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

  private final List<FacetField> facets = new ArrayList<FacetField>();
  private FacetsConfig config;
  
  public AddFacetedDocTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    if (config == null) {
      boolean withFacets = getRunData().getConfig().get("with.facets", true);
      if (withFacets) {
        // nocommit is this called once?  are we adding same
        // facets over and over!?
        FacetSource facetsSource = getRunData().getFacetSource();
        config = new FacetsConfig();
        facetsSource.getNextFacets(facets);
        facetsSource.configure(config);
      }
    }
  }

  @Override
  protected String getLogMessage(int recsCount) {
    if (config == null) {
      return super.getLogMessage(recsCount);
    }
    return super.getLogMessage(recsCount)+ " with facets";
  }
  
  @Override
  public int doLogic() throws Exception {
    if (config != null) {
      // nocommit hokey:
      Document doc2 = new Document();
      for(FacetField ff : facets) {
        doc2.add(ff);
      }
      IndexDocument doc3 = config.build(getRunData().getTaxonomyWriter(), doc2);
      for(StorableField field : doc3.storableFields()) {
        doc.add((Field) field);
      }
      for(IndexableField field : doc3.indexableFields()) {
        doc.add((Field) field);
      }
      
    }
    return super.doLogic();
  }
}
