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

import java.util.List;

/**
 * A split index command encapsulated in an object.
 *
 * @since solr 1.4
 *
 */
public class SplitIndexCommand extends UpdateCommand {
  // public List<Directory> dirs;
  public List<String> paths;
  public List<SolrCore> cores;  // either paths or cores should be specified
  public List<DocRouter.Range> ranges;
  public DocRouter router;
  public String routeFieldName;
  public String splitKey;

  public SplitIndexCommand(SolrQueryRequest req, List<String> paths, List<SolrCore> cores, List<DocRouter.Range> ranges, DocRouter router, String routeFieldName, String splitKey) {
    super(req);
    this.paths = paths;
    this.cores = cores;
    this.ranges = ranges;
    this.router = router;
    this.routeFieldName = routeFieldName;
    this.splitKey = splitKey;
  }

  @Override
  public String name() {
    return "split";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(",paths=" + paths);
    sb.append(",cores=" + cores);
    sb.append(",ranges=" + ranges);
    sb.append(",router=" + router);
    if (routeFieldName != null) {
      sb.append(",routeFieldName=" + routeFieldName);
    }
    if (splitKey != null) {
      sb.append(",split.key=" + splitKey);
    }
    sb.append('}');
    return sb.toString();
  }
}
