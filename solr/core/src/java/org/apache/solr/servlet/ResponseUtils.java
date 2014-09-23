package org.apache.solr.servlet;
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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Response helper methods.
 */
public class ResponseUtils {
  private ResponseUtils() {}

  /**
   * Adds the given Throwable's message to the given NamedList.
   * <p/>
   * If the response code is not a regular code, the Throwable's
   * stack trace is both logged and added to the given NamedList.
   * <p/>
   * Status codes less than 100 are adjusted to be 500.
   */
  public static int getErrorInfo(Throwable ex, NamedList info, Logger log) {
    int code = 500;
    if (ex instanceof SolrException) {
      SolrException solrExc = (SolrException)ex;
      code = solrExc.code();
      NamedList<String> errorMetadata = solrExc.getMetadata();
      if (errorMetadata != null)
        info.add("metadata", errorMetadata);
    }
    
    for (Throwable th = ex; th != null; th = th.getCause()) {
      String msg = th.getMessage();
      if (msg != null) {
        info.add("msg", msg);
        break;
      }
    }
    
    // For any regular code, don't include the stack trace
    if (code == 500 || code < 100) {
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw));
      SolrException.log(log, null, ex);
      info.add("trace", sw.toString());

      // non standard codes have undefined results with various servers
      if (code < 100) {
        log.warn("invalid return code: " + code);
        code = 500;
      }
    }
    
    info.add("code", code);
    return code;
  }
}
