/**
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

package org.apache.solr.update.processor;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;

import java.util.List;

/**
 * Manages a chain of UpdateRequestProcessorFactories.
 * <p>
 * Chain can be configured via solrconfig.xml:
 * </p>
 * <pre>
 * &lt;updateRequestProcessors name="key" default="true"&gt;
 *   &lt;processor class="PathToClass1" /&gt;
 *   &lt;processor class="PathToClass2" /&gt;
 *   &lt;processor class="solr.LogUpdateProcessorFactory" &gt;
 *     &lt;int name="maxNumToLog"&gt;100&lt;/int&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 * &lt;/updateRequestProcessors&gt;
 * </pre>
 *
 * @see UpdateRequestProcessorFactory
 * @since solr 1.3
 */
public final class UpdateRequestProcessorChain implements PluginInfoInitialized
{
  private UpdateRequestProcessorFactory[] chain;
  private final SolrCore solrCore;

  public UpdateRequestProcessorChain(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  public void init(PluginInfo info) {
    List<UpdateRequestProcessorFactory> list = solrCore.initPlugins(info.getChildren("processor"),UpdateRequestProcessorFactory.class,null);
    if(list.isEmpty()){
      throw new RuntimeException( "updateRequestProcessorChain require at least one processor");
    }
    chain = list.toArray(new UpdateRequestProcessorFactory[list.size()]); 
  }

  public UpdateRequestProcessorChain( UpdateRequestProcessorFactory[] chain , SolrCore solrCore) {
    this.chain = chain;
    this.solrCore =  solrCore;
  }

  public UpdateRequestProcessor createProcessor(SolrQueryRequest req, SolrQueryResponse rsp) 
  {
    UpdateRequestProcessor processor = null;
    UpdateRequestProcessor last = null;
    for (int i = chain.length-1; i>=0; i--) {
      processor = chain[i].getInstance(req, rsp, last);
      last = processor == null ? last : processor;
    }
    return last;
  }

  public UpdateRequestProcessorFactory[] getFactories() {
    return chain;
  }
}
