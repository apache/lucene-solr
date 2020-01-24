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
package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;

public class RuntimeUrp extends SimpleUpdateProcessorFactory {
  @Override
  protected void process(AddUpdateCommand cmd, SolrQueryRequest req, SolrQueryResponse rsp) {
    UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessorChain(req.getParams());
    List<String>  names = new ArrayList<>();
    for (UpdateRequestProcessorFactory p : processorChain.getProcessors()) {
      if (p instanceof UpdateRequestProcessorChain.LazyUpdateProcessorFactoryHolder.LazyUpdateRequestProcessorFactory) {
        p = ((UpdateRequestProcessorChain.LazyUpdateProcessorFactoryHolder.LazyUpdateRequestProcessorFactory) p).getDelegate();
      }
      names.add(p.getClass().getSimpleName());
    }
    cmd.solrDoc.addField("processors_s", StrUtils.join(names,'>'));
  }
}
