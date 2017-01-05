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

import java.util.Set;
import java.util.TreeSet;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Factory for {@link DistributedUpdateProcessor}.
 *
 * @see DistributedUpdateProcessor
 */
public class DistributedUpdateProcessorFactory 
  extends UpdateRequestProcessorFactory 
  implements DistributingUpdateProcessorFactory {

  /**
   * By default, the {@link DistributedUpdateProcessor} is extremely conservative in the list of request 
   * params that will be copied/included when updates are forwarded to other nodes.  This method may be 
   * used by any {@link UpdateRequestProcessorFactory#getInstance} call to annotate a 
   * SolrQueryRequest with the names of parameters that should also be forwarded.
   */
  public static void addParamToDistributedRequestWhitelist(final SolrQueryRequest req, final String... paramNames) {
    Set<String> whitelist = (Set<String>) req.getContext().get(DistributedUpdateProcessor.PARAM_WHITELIST_CTX_KEY);
    if (null == whitelist) {
      whitelist = new TreeSet<String>();
      req.getContext().put(DistributedUpdateProcessor.PARAM_WHITELIST_CTX_KEY, whitelist);
    }
    for (String p : paramNames) {
      whitelist.add(p);
    }
  }
  
  @Override
  public void init(NamedList args) {

  }
  
  @Override
  public DistributedUpdateProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {

    return new DistributedUpdateProcessor(req, rsp, next);
  }
  
}
