package org.apache.solr.cloud.rule;

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


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImplicitSnitch extends Snitch implements CoreAdminHandler.Invocable {
  static final Logger log = LoggerFactory.getLogger(ImplicitSnitch.class);

  public static final Pattern hostAndPortPattern = Pattern.compile("(?:https?://)?([^:]+):(\\d+)");

  //well known tags
  public static final String NODE = "node";
  public static final String PORT = "port";
  public static final String HOST = "host";
  public static final String CORES = "cores";
  public static final String DISK = "freedisk";
  public static final String SYSPROP = "sysprop.";

  public static final Set<String> tags = ImmutableSet.of(NODE, PORT, HOST, CORES, DISK);


  @Override
  public void getTags(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
    if (requestedTags.contains(NODE)) ctx.getTags().put(NODE, solrNode);
    if (requestedTags.contains(HOST)) {
      Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
      if (hostAndPortMatcher.find()) ctx.getTags().put(HOST, hostAndPortMatcher.group(1));
    }
    if (requestedTags.contains(PORT)) {
      Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
      if (hostAndPortMatcher.find()) ctx.getTags().put(PORT, hostAndPortMatcher.group(2));
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (requestedTags.contains(CORES)) params.add(CORES, "1");
    if (requestedTags.contains(DISK)) params.add(DISK, "1");
    for (String tag : requestedTags) {
      if (tag.startsWith(SYSPROP)) params.add(SYSPROP, tag.substring(SYSPROP.length()));
    }
    if (params.size() > 0) ctx.invokeRemote(solrNode, params, ImplicitSnitch.class.getName(), null);
  }

  public Map<String, Object> invoke(SolrQueryRequest req) {
    Map<String, Object> result = new HashMap<>();
    if (req.getParams().getInt(CORES, -1) == 1) {
      CoreContainer cc = (CoreContainer) req.getContext().get(CoreContainer.class.getName());
      result.put(CORES, cc.getCoreNames().size());
    }
    if (req.getParams().getInt(DISK, -1) == 1) {
      try {
        long space = Files.getFileStore(Paths.get("/")).getUsableSpace();
        long spaceInGB = space / 1024 / 1024 / 1024;
        result.put(DISK, spaceInGB);
      } catch (IOException e) {

      }
    }
    String[] sysProps = req.getParams().getParams(SYSPROP);
    if (sysProps != null && sysProps.length > 0) {
      for (String prop : sysProps) result.put(SYSPROP+prop, System.getProperty(prop));
    }
    return result;
  }


  @Override
  public boolean isKnownTag(String tag) {
    return tags.contains(tag) ||
        tag.startsWith(SYSPROP);//a system property
  }

}
