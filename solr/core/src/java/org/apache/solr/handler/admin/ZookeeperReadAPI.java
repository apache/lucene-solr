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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import static org.apache.solr.common.params.CommonParams.OMIT_HEADER;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.response.RawResponseWriter.CONTENT;
import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_READ_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.ZK_READ_PERM;

/**
 * Exposes the content of the Zookeeper
 * This is an expert feature that exposes the data inside the back end zookeeper.This API may change or
 * be removed in future versions.
 * This is not a public API. The data that is returned is not guaranteed to remain same
 * across releases, as the data stored in Zookeeper may change from time to time.
 * @lucene.experimental
 */

public class ZookeeperReadAPI {
  private final CoreContainer coreContainer;
  private final SolrParams rawWtParams;

  public ZookeeperReadAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    Map<String, String> map = new HashMap<>(1);
    map.put(WT, "raw");
    map.put(OMIT_HEADER, "true");
    rawWtParams = new MapSolrParams(map);
  }

  /**
   * Request contents of a znode, except security.json
   */
  @EndPoint(path = "/cluster/zk/data/*",
      method = SolrRequest.METHOD.GET,
      permission = ZK_READ_PERM)
  public void readNode(SolrQueryRequest req, SolrQueryResponse rsp) {
    String path = req.getPathTemplateValues().get("*");
    if (path == null || path.isEmpty()) path = "/";
    readNodeAndAddToResponse(path, req, rsp);
  }

  /**
   * Request contents of the security.json node
   */
  @EndPoint(path = "/cluster/zk/data/security.json",
      method = SolrRequest.METHOD.GET,
      permission = SECURITY_READ_PERM)
  public void readSecurityJsonNode(SolrQueryRequest req, SolrQueryResponse rsp) {
    String path = "/security.json";
    readNodeAndAddToResponse(path, req, rsp);
  }

  /**
   * List the children of a certain zookeeper znode
   */
  @EndPoint(path = "/cluster/zk/ls/*",
      method = SolrRequest.METHOD.GET,
      permission = ZK_READ_PERM)
  public void listNodes(SolrQueryRequest req, SolrQueryResponse rsp) {
    String path = req.getPathTemplateValues().get("*");
    if (path == null || path.isEmpty()) path = "/";
    try {
      Stat stat = coreContainer.getZkController().getZkClient().exists(path, null, true);
      rsp.add("stat", (MapWriter) ew -> printStat(ew, stat));
      if(!req.getParams().getBool("c", true)) {
        return;
      }
      List<String> l = coreContainer.getZkController().getZkClient().getChildren(path, null, false);
      String prefix = path.endsWith("/") ? path : path + "/";

      Map<String , Stat> stats = new LinkedHashMap<>();
      for (String s : l) {
        try {
          stats.put(s, coreContainer.getZkController().getZkClient().exists(prefix + s, null, false));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      rsp.add(path, (MapWriter) ew -> {
        for (Map.Entry<String, Stat> e : stats.entrySet()) {
          ew.put(e.getKey(), (MapWriter) ew1 -> printStat(ew1, e.getValue()));
        }
      });
    } catch (KeeperException.NoNodeException e) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such node :"+ path);
    } catch (Exception e) {
      rsp.add(CONTENT, new ContentStreamBase.StringStream(Utils.toJSONString(Collections.singletonMap("error", e.getMessage()))));
    } finally {
      RequestHandlerUtils.addExperimentalFormatWarning(rsp);
    }
  }

  /**
   * Simple mime type guessing based on first character of the response
   */
  private String guessMime(byte firstByte) {
    switch(firstByte) {
      case '{':
        return CommonParams.JSON_MIME;
      case '<':
      case '?':
        return XMLResponseParser.XML_CONTENT_TYPE;
      default:
        return BinaryResponseParser.BINARY_CONTENT_TYPE;
    }
  }

  /**
   * Reads content of a znode and adds it to the response
   */
  private void readNodeAndAddToResponse(String zkPath, SolrQueryRequest req, SolrQueryResponse rsp) {
    byte[] d = readPathFromZookeeper(zkPath);
    if (d == null || d.length == 0) {
      rsp.add(zkPath, null);
      return;
    }
    req.setParams(SolrParams.wrapDefaults(rawWtParams, req.getParams()));
    rsp.add(CONTENT, new ContentStreamBase.ByteArrayStream(d, null, guessMime(d[0])));
  }

  /**
   * Reads a single node from zookeeper and return as byte array
   */
  private byte[] readPathFromZookeeper(String path) {
    byte[] d;
    try {
      d = coreContainer.getZkController().getZkClient().getData(path, null, null, false);
    } catch (KeeperException.NoNodeException e) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such node: " + path);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unexpected error", e);
    }
    return d;
  }

  private void printStat(MapWriter.EntryWriter ew, Stat stat) throws IOException {
    ew.put("version", stat.getVersion());
    ew.put("aversion", stat.getAversion());
    ew.put("children", stat.getNumChildren());
    ew.put("ctime", stat.getCtime());
    ew.put("cversion", stat.getCversion());
    ew.put("czxid", stat.getCzxid());
    ew.put("ephemeralOwner", stat.getEphemeralOwner());
    ew.put("mtime", stat.getMtime());
    ew.put("mzxid", stat.getMzxid());
    ew.put("pzxid", stat.getPzxid());
    ew.put("dataLength", stat.getDataLength());
  }

}
