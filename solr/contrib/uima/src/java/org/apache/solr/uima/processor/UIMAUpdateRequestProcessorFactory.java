package org.apache.solr.uima.processor;

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

import org.apache.lucene.analysis.uima.ae.AEProvider;
import org.apache.lucene.analysis.uima.ae.AEProviderFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.JCasPool;

/**
 * Factory for {@link UIMAUpdateRequestProcessor}
 * 
 * 
 */
public class UIMAUpdateRequestProcessorFactory extends
    UpdateRequestProcessorFactory {

  private NamedList<Object> args;
  private AnalysisEngine ae;
  private JCasPool pool;

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    this.args = (NamedList<Object>) args.get("uimaConfig");
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    SolrUIMAConfiguration configuration = new SolrUIMAConfigurationReader(args)
        .readSolrUIMAConfiguration();
    synchronized (this) {
      if (ae == null && pool == null) {
        AEProvider aeProvider = AEProviderFactory.getInstance().getAEProvider(
            req.getCore().getName(), configuration.getAePath(),
            configuration.getRuntimeParameters());
        try {
          ae = aeProvider.getAE();
          pool = new JCasPool(10, ae);
        } catch (ResourceInitializationException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
    }
    
    return new UIMAUpdateRequestProcessor(next, req.getCore().getName(),
        configuration, ae, pool);
  }
  
}
