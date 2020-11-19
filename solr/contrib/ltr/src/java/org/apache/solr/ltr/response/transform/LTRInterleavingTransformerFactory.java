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
package org.apache.solr.ltr.response.transform;

import java.io.IOException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.ltr.interleaving.LTRInterleavingScoringQuery;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.SolrQueryRequestContextUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.util.SolrPluginUtils;

public class LTRInterleavingTransformerFactory extends TransformerFactory {
  
  @Override
  @SuppressWarnings({"unchecked"})
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    super.init(args);
    SolrPluginUtils.invokeSetters(this, args);
  }

  @Override
  public DocTransformer create(String name, SolrParams localparams,
      SolrQueryRequest req) {
    return new InterleavingTransformer(name, req);
  }
  
  class InterleavingTransformer extends DocTransformer {

    final private String name;
    final private SolrQueryRequest req;
    
    private LTRInterleavingScoringQuery[] rerankingQueries;

    /**
     * @param name
     *          Name of the field to be added in a document representing the
     *          model picked by the interleaving process
     */
    public InterleavingTransformer(String name,
        SolrQueryRequest req) {
      this.name = name;
      this.req = req;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void setContext(ResultContext context) {
      super.setContext(context);
      if (context == null) {
        return;
      }
      if (context.getRequest() == null) {
        return;
      }
      rerankingQueries = (LTRInterleavingScoringQuery[])SolrQueryRequestContextUtils.getScoringQueries(req);
      for (int i = 0; i < rerankingQueries.length; i++) {
        LTRScoringQuery scoringQuery = rerankingQueries[i];

        if (scoringQuery.getOriginalQuery() == null) {
          scoringQuery.setOriginalQuery(context.getQuery());
        }
        if (scoringQuery.getFeatureLogger() == null) {
          scoringQuery.setFeatureLogger( SolrQueryRequestContextUtils.getFeatureLogger(req) );
        }
        scoringQuery.setRequest(req);
      }
    }

    @Override
    public void transform(SolrDocument doc, int docid, float score)
        throws IOException {
      implTransform(doc, docid);
    }

    @Override
    public void transform(SolrDocument doc, int docid)
        throws IOException {
      implTransform(doc, docid);
    }

    private void implTransform(SolrDocument doc, int docid) {
      LTRScoringQuery rerankingQuery = rerankingQueries[0];
      if (rerankingQueries.length > 1 && rerankingQueries[1].getPickedInterleavingDocIds().contains(docid)) {
        rerankingQuery = rerankingQueries[1];
      }
      doc.addField(name, rerankingQuery.getScoringModelName());
    }
  }

}
