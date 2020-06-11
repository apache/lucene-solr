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
package org.apache.solr.security;

import java.security.Principal;
import java.util.Enumeration;
import java.util.List;

import org.apache.solr.common.params.SolrParams;

/**
 * Request context for Solr to be used by Authorization plugin.
 */
public abstract class AuthorizationContext {

  public static class CollectionRequest {
    final public String collectionName;

    public CollectionRequest(String collectionName) {
      this.collectionName = collectionName;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()+ "(" + collectionName + ")";
    }
  }
  
  public abstract SolrParams getParams() ;
  
  public abstract Principal getUserPrincipal() ;

  public abstract String getHttpHeader(String header);
  
  public abstract Enumeration getHeaderNames();

  public abstract String getRemoteAddr();

  public abstract String getRemoteHost();

  public abstract List<CollectionRequest> getCollectionRequests() ;
  
  public abstract RequestType getRequestType();
  
  public abstract String getResource();

  public abstract String getHttpMethod();

  public enum RequestType {READ, WRITE, ADMIN, UNKNOWN}

  public abstract Object getHandler();

}