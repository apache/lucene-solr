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

package org.apache.solr.client.solrj.util;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.Attributes;
import org.eclipse.jetty.util.B64Code;

/**
 * BasicAuthentication that does not care about uri and realm
 */
public class SolrBasicAuthentication implements Authentication {

  private final String value;

  public SolrBasicAuthentication(String user, String password) {
    this.value = "Basic " + B64Code.encode(user + ":" + password, StandardCharsets.ISO_8859_1);
  }

  @Override
  public boolean matches(String type, URI uri, String realm) {
    return true;
  }

  @Override
  public Result authenticate(Request request, ContentResponse response, HeaderInfo headerInfo, Attributes context) {
    return new Result() {
      @Override
      public URI getURI() {
        // cache result by host and port
        return URI.create(String.format(Locale.ROOT, "%s://%s:%d", request.getScheme(), request.getHost(), request.getPort()));
      }

      @Override
      public void apply(Request request) {
        request.header(headerInfo.getHeader(), value);
      }
    };
  }
}
