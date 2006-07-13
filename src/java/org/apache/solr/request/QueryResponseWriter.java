/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.request;

import java.io.Writer;
import java.io.IOException;

/**
 * @author yonik
 * @version $Id$
 */
public interface QueryResponseWriter {
  public static String CONTENT_TYPE_XML_UTF8="text/xml;charset=UTF-8";
  public static String CONTENT_TYPE_TEXT_UTF8="text/plain;charset=UTF-8";
  public static String CONTENT_TYPE_TEXT_ASCII="text/plain;charset=US-ASCII";

  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException;
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response);
}

