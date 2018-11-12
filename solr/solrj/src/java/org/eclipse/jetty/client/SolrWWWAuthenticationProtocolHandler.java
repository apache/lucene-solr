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

package org.eclipse.jetty.client;

import java.net.URI;

import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;

/**
 * <p>A protocol handler that handles the 401 response code
 * in association with the {@code WWW-Authenticate} header.</p>
 *
 * @see ProxyAuthenticationProtocolHandler
 */
public class SolrWWWAuthenticationProtocolHandler extends SolrAuthenticationProtocolHandler
{
  public static final String NAME = "www-authenticate";
  private static final String ATTRIBUTE = SolrWWWAuthenticationProtocolHandler.class.getName() + ".attribute";

  public SolrWWWAuthenticationProtocolHandler(HttpClient client)
  {
    this(client, DEFAULT_MAX_CONTENT_LENGTH);
  }

  public SolrWWWAuthenticationProtocolHandler(HttpClient client, int maxContentLength)
  {
    super(client, maxContentLength);
  }

  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public boolean accept(Request request, Response response)
  {
    return response.getStatus() == HttpStatus.UNAUTHORIZED_401;
  }

  @Override
  protected HttpHeader getAuthenticateHeader()
  {
    return HttpHeader.WWW_AUTHENTICATE;
  }

  @Override
  protected HttpHeader getAuthorizationHeader()
  {
    return HttpHeader.AUTHORIZATION;
  }

  @Override
  protected URI getAuthenticationURI(Request request)
  {
    return request.getURI();
  }

  @Override
  protected String getAuthenticationAttribute()
  {
    return ATTRIBUTE;
  }
}

