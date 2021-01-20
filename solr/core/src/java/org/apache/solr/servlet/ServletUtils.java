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

package org.apache.solr.servlet;

import javax.servlet.http.HttpServletRequest;

/**
 * Various Util methods for interaction on servlet level, i.e. HttpServletRequest
 */
public abstract class ServletUtils {
  private ServletUtils() { /* only static methods in this class */ }

  /**
   * Use this to get the full path after context path "/solr", which is a combination of
   * servletPath and pathInfo.
   * @param request the HttpServletRequest object
   * @return String with path starting with a "/", or empty string if no path
   */
  public static String getPathAfterContext(HttpServletRequest request) {
    return request.getServletPath() + (request.getPathInfo() != null ? request.getPathInfo() : "");
  }
}
