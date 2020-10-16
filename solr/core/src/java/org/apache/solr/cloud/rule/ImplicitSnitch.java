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

package org.apache.solr.cloud.rule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;

import static org.apache.solr.common.cloud.rule.ImplicitSnitch.CORES;
import static org.apache.solr.common.cloud.rule.ImplicitSnitch.DISK;
import static org.apache.solr.common.cloud.rule.ImplicitSnitch.SYSPROP;

//this is the server-side component which provides the tag values

/** @deprecated to be removed in Solr 9.0 (see SOLR-14930)
 *
 */
public class ImplicitSnitch implements CoreAdminHandler.Invocable {

  static long getUsableSpaceInGB(Path path) throws IOException {
    long space = Files.getFileStore(path).getUsableSpace();
    long spaceInGB = space / 1024 / 1024 / 1024;
    return spaceInGB;
  }

  @Override
  public Map<String, Object> invoke(SolrQueryRequest req) {
    Map<String, Object> result = new HashMap<>();
    CoreContainer cc = (CoreContainer) req.getContext().get(CoreContainer.class.getName());
    if (req.getParams().getInt(CORES, -1) == 1) {
      result.put(CORES, cc.getLoadedCoreNames().size());
    }
    if (req.getParams().getInt(DISK, -1) == 1) {
      try {
        final long spaceInGB = getUsableSpaceInGB(cc.getCoreRootDirectory());
        result.put(DISK, spaceInGB);
      } catch (IOException e) {

      }
    }
    String[] sysProps = req.getParams().getParams(SYSPROP);
    if (sysProps != null && sysProps.length > 0) {
      for (String prop : sysProps) result.put(SYSPROP + prop, System.getProperty(prop));
    }
    return result;
  }

}
