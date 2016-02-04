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
package org.apache.solr.client.solrj;

/** Exception to catch all types of communication / parsing issues associated with talking to SOLR
 * 
 *
 * @since solr 1.3
 */
public class SolrServerException extends Exception {

  private static final long serialVersionUID = -3371703521752000294L;
  
  public SolrServerException(String message, Throwable cause) {
    super(message, cause);
  }

  public SolrServerException(String message) {
    super(message);
  }

  public SolrServerException(Throwable cause) {
    super(cause);
  }
  
  public Throwable getRootCause() {
    Throwable t = this;
    while (true) {
      Throwable cause = t.getCause();
      if (cause!=null) {
        t = cause;
      } else {
        break;
      }
    }
    return t;
  }

}
