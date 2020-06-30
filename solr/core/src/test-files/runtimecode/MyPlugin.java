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

package runtimecode;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

@EndPoint(path = "/plugin/my/path",
    method = METHOD.GET,
    permission = PermissionNameProvider.Name.CONFIG_READ_PERM)
public class MyPlugin {

  private final CoreContainer coreContainer;

  public MyPlugin(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Command
  public void call(SolrQueryRequest req, SolrQueryResponse rsp){
    rsp.add("myplugin.version", "2.0");
  }
}
