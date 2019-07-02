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

package org.apache.solr.util.tracing;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import io.opentracing.propagation.TextMap;

/**
 * A Carrier for extract Span context out of request headers
 */
public class HttpServletCarrier implements TextMap {
  private Iterator<Map.Entry<String, String>> it;

  public HttpServletCarrier(HttpServletRequest request) {
    this.it = new Iterator<>() {

      Enumeration<String> headerNameIt = request.getHeaderNames();
      String headerName = null;
      Enumeration<String> headerValue = null;

      @Override
      public boolean hasNext() {
        if (headerValue != null && headerValue.hasMoreElements()) {
          return true;
        }

        return headerNameIt.hasMoreElements();
      }

      @Override
      public Map.Entry<String, String> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        if (headerValue == null || !headerValue.hasMoreElements()) {
          headerName = headerNameIt.nextElement();
          headerValue = request.getHeaders(headerName);
        }

        String key = headerName;
        String val = headerValue.nextElement();

        return new Map.Entry<>() {
          @Override
          public String getKey() {
            return key;
          }

          @Override
          public String getValue() {
            return val;
          }

          @Override
          public String setValue(String value) {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return it;
  }

  @Override
  public void put(String key, String value) {
    throw new UnsupportedOperationException("HttpServletCarrier should only be used with Tracer.extract()");
  }
}
