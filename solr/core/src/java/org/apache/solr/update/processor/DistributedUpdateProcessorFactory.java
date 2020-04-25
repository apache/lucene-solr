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

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Factory for {@link DistributedUpdateProcessor}.
 *
 * @see DistributedUpdateProcessor
 * @since 4.0.0
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
  @SuppressWarnings("unchecked")
  public static void addParamToDistributedRequestWhitelist(final SolrQueryRequest req, final String... paramNames) {
    Set<String> whitelist = (Set<String>) req.getContext()
        .computeIfAbsent(DistributedUpdateProcessor.PARAM_WHITELIST_CTX_KEY, key -> new TreeSet<>());
    Collections.addAll(whitelist, paramNames);
  }
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {

    final boolean isZkAware = req.getCore().getCoreContainer().isZooKeeperAware();

    DistributedUpdateProcessor distribUpdateProcessor =
        isZkAware ?
            new DistributedZkUpdateProcessor(req, rsp, next) :
            new DistributedUpdateProcessor(req, rsp, next);
    // note: will sometimes return DURP (no overhead) instead of wrapping
    return RoutedAliasUpdateProcessor.wrap(req,
        distribUpdateProcessor);
  }
  
}
