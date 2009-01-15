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

package org.apache.solr.client.solrj.request;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.ContentStream;

import java.io.IOException;
import java.util.Collection;

/**
 * A RequestWriter is used to write requests to Solr.
 * <p/>
 * A subclass can override the methods in this class to supply a custom format in which a request can be sent.
 * @since solr 1.4
 * @version $Id$
 */
public class RequestWriter {

  public Collection<ContentStream> getContentStreams(SolrRequest req) throws IOException {
    return req.getContentStreams();
  }

  public String getPath(SolrRequest req) {
    return req.getPath();
  }

}
