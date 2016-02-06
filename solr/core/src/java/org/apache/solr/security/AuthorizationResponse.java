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

/* This class currently only stores an int statusCode (HttpStatus) value and a message but can 
   be used to return ACLs and other information from the authorization plugin.
 */
public class AuthorizationResponse {
  public static final AuthorizationResponse OK = new AuthorizationResponse(200);
  public static final AuthorizationResponse FORBIDDEN = new AuthorizationResponse(403);
  public static final AuthorizationResponse PROMPT = new AuthorizationResponse(401);
  public final int statusCode;
  String message;

  public AuthorizationResponse(int httpStatusCode) {
    this.statusCode = httpStatusCode;
  }
  
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
