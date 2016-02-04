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
package org.apache.solr.update.processor;

import java.io.IOException;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;


/**
 * A base class for writing a very simple UpdateProcessor without worrying too much about the API.
 * This is deliberately made to support only the add operation
 */
public abstract class SimpleUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  @Override
  public UpdateRequestProcessor getInstance(final SolrQueryRequest req, final SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        process(cmd, req, rsp);
        super.processAdd(cmd);
      }
    };
  }

  /**
   * This object is reused across requests. So,this method should not store anything in the instance variable.
   */
  protected abstract void process(AddUpdateCommand cmd, SolrQueryRequest req, SolrQueryResponse rsp);
}
