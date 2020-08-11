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


  /**
   * This method returns the {@link Principal} corresponding to
   * the authenticated user for the current request. The value returned by
   * {@link Principal#getName()} depends on the authentication mechanism
   * used (e.g. for user "foo" with BASIC authentication the result would be
   * "foo". On the other hand with KERBEROS it would be foo@REALMNAME).
   * The {@link #getUserName()} method may be preferred to extract the identity
   * of the authenticated user instead of this method.
   *
   * @return user principal in case of an authenticated request
   *         null in case of unauthenticated request
   */
  public abstract Principal getUserPrincipal() ;

  /**
   * This method returns the name of the authenticated user for the current request.
   * The return value of this method is agnostic of the underlying authentication
   * mechanism used.
   *
   * @return user name in case of an authenticated user
   *         null in case of unauthenticated request
   */
  public abstract String getUserName();

  public abstract String getHttpHeader(String header);
  
  public abstract Enumeration<String> getHeaderNames();

  public abstract String getRemoteAddr();

  public abstract String getRemoteHost();

  public abstract List<CollectionRequest> getCollectionRequests() ;
  
  public abstract RequestType getRequestType();
  
  public abstract String getResource();

  public abstract String getHttpMethod();

  public enum RequestType {READ, WRITE, ADMIN, UNKNOWN}

  public abstract Object getHandler();

}