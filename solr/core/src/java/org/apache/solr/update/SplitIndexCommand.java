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
package org.apache.solr.update;

import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.List;

/**
 * A split index command encapsulated in an object.
 *
 * @since solr 1.4
 *
 */
public class SplitIndexCommand extends UpdateCommand {
  public final SolrQueryResponse rsp;
  public final List<String> paths;
  public final List<SolrCore> cores;  // either paths or cores should be specified
  public final List<DocRouter.Range> ranges;
  public final DocRouter router;
  public final String routeFieldName;
  public final String splitKey;
  public final SolrIndexSplitter.SplitMethod splitMethod;

  public SplitIndexCommand(SolrQueryRequest req, SolrQueryResponse rsp, List<String> paths, List<SolrCore> cores, List<DocRouter.Range> ranges,
                           DocRouter router, String routeFieldName, String splitKey, SolrIndexSplitter.SplitMethod splitMethod) {
    super(req);
    this.rsp = rsp;
    this.paths = paths;
    this.cores = cores;
    this.ranges = ranges;
    this.router = router;
    this.routeFieldName = routeFieldName;
    this.splitKey = splitKey;
    this.splitMethod = splitMethod;
  }

  @Override
  public String name() {
    return "split";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(",paths=").append(paths);
    sb.append(",cores=").append(cores);
    sb.append(",ranges=").append(ranges);
    sb.append(",router=").append(router);
    if (routeFieldName != null) {
      sb.append(",routeFieldName=").append(routeFieldName);
    }
    if (splitKey != null) {
      sb.append(",split.key=").append(splitKey);
    }
    sb.append(",method=").append(splitMethod.toLower());
    sb.append('}');
    return sb.toString();
  }
}
