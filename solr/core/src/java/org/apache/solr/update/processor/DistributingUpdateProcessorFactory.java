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

import org.apache.solr.common.SolrException;

/**
 * A marker interface for denoting that a factory is responsible for handling
 * distributed communication of updates across a SolrCloud cluster.
 * 
 * @see UpdateRequestProcessorChain#init
 * @see UpdateRequestProcessorChain#createProcessor
 */
public interface DistributingUpdateProcessorFactory {

  /**
   * Internal param used to specify the current phase of a distributed update, 
   * not intended for use by clients.  Any non-blank value can be used to 
   * indicate to the <code>UpdateRequestProcessorChain</code> that factories 
   * prior to the <code>DistributingUpdateProcessorFactory</code> can be skipped.
   * Implementations of this interface may use the non-blank values any way 
   * they wish.
   */
  public static final String DISTRIB_UPDATE_PARAM = "update.distrib";
}
