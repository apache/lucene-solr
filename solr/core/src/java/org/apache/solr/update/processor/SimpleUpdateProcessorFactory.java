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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;


/**
 * A base class for writing a very simple UpdateProcessor without worrying too much about the API.
 * This is deliberately made to support only the add operation
 * @since 5.1.0
 */
public abstract class SimpleUpdateProcessorFactory extends UpdateRequestProcessorFactory {
  private String myName; // if classname==XyzUpdateProcessorFactory  myName=Xyz
  @SuppressWarnings({"rawtypes"})
  protected NamedList initArgs = new NamedList();
  private static ThreadLocal<SolrQueryRequest> REQ = new ThreadLocal<>();

  protected SimpleUpdateProcessorFactory() {

  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);
    this.initArgs = args;

  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        REQ.set(req);
        try {
          process(cmd, req, rsp);
        } finally {
          REQ.remove();
        }
        super.processAdd(cmd);
      }
    };
  }

  protected String getParam(String name) {
    String[] v = getParams(name);
    return v == null || v.length == 0 ? null : v[0];
  }

  /**returns value from init args or request parameter. the request parameter must have the
   * URP shortname prefixed
   */
  protected String[] getParams(String name) {
    Object v = REQ.get().getParams().getParams(_param(name));
    if (v == null) v = initArgs.get(name);
    if (v == null) return null;
    if (v instanceof String[]) return (String[]) v;
    return new String[]{v.toString()};

  }

  private String _param(String name) {
    return getMyName() + "." + name;
  }

  protected String getMyName() {
    String myName = this.myName;
    if (myName == null) {
      String simpleName = this.getClass().getSimpleName();
      int idx = simpleName.indexOf("UpdateProcessorFactory");
      this.myName = myName = idx == -1 ? simpleName : simpleName.substring(0, idx);
    }
    return myName;
  }


  /**
   * This object is reused across requests. So,this method should not store anything in the instance variable.
   */
  protected abstract void process(AddUpdateCommand cmd, SolrQueryRequest req, SolrQueryResponse rsp);
}
