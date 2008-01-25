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

package org.apache.solr.client.solrj;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public abstract class SolrRequest implements Serializable
{
  public enum METHOD {
    GET,
    POST
  };

  private METHOD method = METHOD.GET;
  private String path = null;
  private String core = null;

  //---------------------------------------------------------
  //---------------------------------------------------------

  public SolrRequest( METHOD m, String path )
  {
    this.method = m;
    this.path = path;
  }

  //---------------------------------------------------------
  //---------------------------------------------------------

  public METHOD getMethod() {
    return method;
  }
  public void setMethod(METHOD method) {
    this.method = method;
  }

  public String getPath() {
    return path;
  }
  public void setPath(String path) {
    this.path = path;
  }

  public String getCore() {
    return core;
  }

  public void setCore(String core) {
    this.core = core;
  }


  public abstract SolrParams getParams();
  public abstract Collection<ContentStream> getContentStreams() throws IOException;
  public abstract SolrResponse process( SolrServer server ) throws SolrServerException, IOException;
}
