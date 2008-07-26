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
import org.apache.solr.request.SolrQueryResponse;

/**
 * An UpdateRequestProcessorFactory that constructs a chain of UpdateRequestProcessor.
 * 
 * This is the default implementation and can be configured via solrconfig.xml with:
 * 
 * <updateRequestProcessors name="key" default="true">
 *   <processor class="PathToClass1" />
 *   <processor class="PathToClass2" />
 *   <processor class="solr.LogUpdateProcessorFactory" >
 *     <int name="maxNumToLog">100</int>
 *   </processor>
 *   <processor class="solr.RunUpdateProcessorFactory" />
 * </updateRequestProcessors>
 * 
 * @since solr 1.3
 */
public final class UpdateRequestProcessorChain 
{
  final UpdateRequestProcessorFactory[] chain;
  
  public UpdateRequestProcessorChain( UpdateRequestProcessorFactory[] chain ) {
    this.chain = chain;
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
