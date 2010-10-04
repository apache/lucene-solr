/**
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
package org.apache.solr.request;

import java.io.Writer;

import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @deprecated use org.apache.solr.response.TextResponseWriter
 */
@Deprecated
public abstract class TextResponseWriter extends org.apache.solr.response.TextResponseWriter 
{
  private static Logger log = LoggerFactory.getLogger(TextResponseWriter.class.getName());

  public TextResponseWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
    log.warn(TextResponseWriter.class.getName()+" is deprecated. Please use the corresponding class in org.apache.solr.response");    
  }
}
