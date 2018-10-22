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
package org.apache.solr.client.solrj.response;

import java.util.Date;
import org.apache.solr.common.util.NamedList;

/**
 * 
 *
 * @since solr 1.3
 */
public class CoreAdminResponse extends SolrResponseBase
{ 
  @SuppressWarnings("unchecked")
  public NamedList<NamedList<Object>> getCoreStatus()
  {
    return (NamedList<NamedList<Object>>) getResponse().get( "status" );
  }

  public NamedList<Object> getCoreStatus( String core )
  {
    return getCoreStatus().get( core );
  }
  
  public Date getStartTime( String core )
  {
    NamedList<Object> v = getCoreStatus( core );
    if( v == null ) {
      return null;
    }
    return (Date) v.get( "startTime" );
  }
  
  public Long getUptime( String core )
  {
    NamedList<Object> v = getCoreStatus( core );
    if( v == null ) {
      return null;
    }
    return (Long) v.get( "uptime" );
  }
}