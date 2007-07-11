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

package org.apache.solr.handler;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A factory to generate UpdateRequestProcessors for each request.  The default
 * implementation does nothing except pass the commands directly to the 
 * UpdateHandler
 * 
 * @since solr 1.3
 */
public class UpdateRequestProcessorFactory 
{
  public UpdateRequestProcessorFactory()
  {
    
  }
  
  public void init( NamedList<Object> args )
  {
    // by default nothing...
  }
  
  public UpdateRequestProcessor getInstance( SolrQueryRequest req )
  {
    return new UpdateRequestProcessor( req );
  }
}
