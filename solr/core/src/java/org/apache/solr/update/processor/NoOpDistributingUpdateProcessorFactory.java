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

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * A No-Op implementation of DistributingUpdateProcessorFactory that 
 * allways returns the next processor instead of inserting a new URP in front of it.
 * <p> 
 * This implementation may be useful for Solr installations in which neither 
 * the <code>{@link DistributedUpdateProcessorFactory}</code> nor any custom 
 * implementation of <code>{@link DistributingUpdateProcessorFactory}</code> 
 * is desired (ie: shards are managed externally from Solr)
 * </p>
 * @since 4.0.0
 */
public class NoOpDistributingUpdateProcessorFactory 
  extends UpdateRequestProcessorFactory 
  implements DistributingUpdateProcessorFactory {    

  /** Returns the next
   */
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next ) {
    return next;
  }
}
