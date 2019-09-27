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

package org.apache.solr.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.http.client.HttpClient;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.api.CallInfo;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.MapWriter.EMPTY;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.core.BlobRepository.sha256Digest;
import static org.apache.solr.core.RuntimeLib.SHA256;
import static org.apache.solr.handler.ReplicationHandler.FILE_STREAM;

/**
 * This class represents the new P2P, File System Blob Store.
 * This identifies a blob by its sha256.
 * This acts as a server for blobs for a user or any other node to
 * download a blob.
 * This also is responsioble for distributing a blob across the nodes
 * in the cluster.
 */

public class FsBlobStore {
  static final long MAX_PKG_SIZE = Long.parseLong(System.getProperty("max.package.size", String.valueOf(100 * 1024 * 1024)));

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;

  private Map<String, ByteBuffer> tmpBlobs = new ConcurrentHashMap<>();

  final BlobRead blobRead = new BlobRead();

  public FsBlobStore(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  public MapWriter fileList(SolrParams params) {
    String sha256 = params.get(SHA256);
    File dir = getBlobsPath().toFile();

    String fromNode = params.get("fromNode");
    if (sha256 != null && fromNode != null) {
      //asking to fetch it from somewhere if it does not exist locally
      if (!new File(dir, sha256).exists()) {
        if ("*".equals(fromNode)) {
          //asking to fetch from a random node
          fetchFromOtherNodes(sha256);
          return EMPTY;
        } else { // asking to fetch from a specific node
          fetchBlobFromNodeAndPersist(sha256, fromNode);
          return MapWriter.EMPTY;
        }
      }
    }
    return ew -> dir.listFiles((f, name) -> {
      if (sha256 == null || name.equals(sha256)) {
        ew.putNoEx(name, (MapWriter) ew1 -> {
          File file = new File(f, name);
          ew1.put("size", file.length());
          ew1.put("timestamp", new Date(file.lastModified()));
        });
      }
      return false;
    });
  }

  public Path getBlobsPath() {
    return SolrResourceLoader.getBlobsDirPath(this.coreContainer.getResourceLoader().getInstancePath());
  }

  private ByteBuffer fetchFromOtherNodes(String sha256) {
    ByteBuffer[] result = new ByteBuffer[1];
    ArrayList<String> l = shuffledNodes();
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(SHA256, sha256);
    ZkStateReader stateReader = coreContainer.getZkController().getZkStateReader();
    for (String liveNode : l) {
      try {
        String baseurl = stateReader.getBaseUrlForNodeName(liveNode);
        String url = baseurl.replace("/solr", "/api");
        String reqUrl = url + "/node/blob?wt=javabin&omitHeader=true&sha256=" + sha256;
        boolean nodeHasBlob = false;
        Object nl = Utils.executeGET(coreContainer.getUpdateShardHandler().getDefaultHttpClient(), reqUrl, Utils.JAVABINCONSUMER);
        if (Utils.getObjectByPath(nl, false, Arrays.asList("blob", sha256)) != null) {
          nodeHasBlob = true;
        }

        if (nodeHasBlob) {
          result[0] = fetchBlobFromNodeAndPersist(sha256, liveNode);
          if (result[0] != null) break;
        }
      } catch (Exception e) {
        //it's OK for some nodes to fail
      }
    }

    return result[0];
  }

  /**
   * get a list of nodes randomly shuffled
   * * @lucene.internal
   */
  public ArrayList<String> shuffledNodes() {
    Set<String> liveNodes = coreContainer.getZkController().getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList(liveNodes);
    Collections.shuffle(l, BlobRepository.RANDOM);
    return l;
  }


  private void persistToFile(ByteBuffer b, String sha256) throws IOException {
    String actual = sha256Digest(b);
    if (!Objects.equals(actual, sha256)) {
      throw new SolrException(SERVER_ERROR, "invalid sha256 for blob actual: " + actual + " expected : " + sha256);
    }
    File file = new File(getBlobsPath().toFile(), sha256);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(b.array(), 0, b.limit());
    }
    log.info("persisted a blob {} ", sha256);
    IOUtils.fsync(file.toPath(), false);
  }


  boolean fetchBlobToFS(String sha256) {
    File f = new File(getBlobsPath().toFile(), sha256);
    if (f.exists()) return true;
    fetchFromOtherNodes(sha256);
    return f.exists();
  }

  /**
   * Read a blob from the blobstore file system
   */
  public void readBlob(String sha256, Consumer<InputStream> consumer) throws IOException {
    if (!fetchBlobToFS(sha256)) throw new FileNotFoundException("No such blob: "+ sha256);
    File f = new File(getBlobsPath().toFile(), sha256);
    try (InputStream is = new FileInputStream(f)) {
      consumer.accept(is);
    }
  }

  /**
   * This distributes a blob to all nodes in the cluster
   * *USE CAREFULLY*
   */
  public void distributeBlob(ByteBuffer buf, String sha256) throws IOException {
    persistToFile(buf, sha256);
    tmpBlobs.put(sha256, buf);
    List<String> nodes = coreContainer.getBlobStore().shuffledNodes();
    int i = 0;
    int FETCHFROM_SRC = 50;
    try {
      for (String node : nodes) {
        String baseUrl = coreContainer.getZkController().getZkStateReader().getBaseUrlForNodeName(node);
        String url = baseUrl.replace("/solr", "/api") + "/node/blob?sha256=" + sha256 + "&fromNode=";
        if (i < FETCHFROM_SRC) {
          // this is to protect very large clusters from overwhelming a single node
          // the first FETCHFROM_SRC nodes will be asked to fetch from this node.
          // it's there in  the memory now. So , it must be served fast
          url += coreContainer.getZkController().getNodeName();
        } else {
          if (i == FETCHFROM_SRC) {
            // This is just an optimization
            // at this point a bunch of nodes are already downloading from me
            // I'll wait for them to finish before asking other nodes to download from each other
            try {
              Thread.sleep(2 * 1000);
            } catch (Exception e) {
            }
          }
          // trying to avoid the thundering herd problem when there are a very large no:of nodes
          // others should try to fetch it from any node where it is available. By now,
          // almost FETCHFROM_SRC other nodes may have it
          url += "*";
        }
        try {
          //fire and forget
          Utils.executeGET(coreContainer.getUpdateShardHandler().getDefaultHttpClient(), url, null);
        } catch (Exception e) {
          log.info("Node: " + node +
              " failed to respond for blob notification", e);
          //ignore the exception
          // some nodes may be down or not responding
        }
        i++;
      }
    } finally {
      new Thread(() -> {
        try {
          // keep the jar in memory for 10 secs , so that
          //every node can download it from memory without the file system
          Thread.sleep(10 * 1000);
        } catch (Exception e) {
          //don't care
        } finally {
          coreContainer.getBlobStore().tmpBlobs.remove(sha256);
        }
      }).start();


    }

  }


  private ByteBuffer fetchBlobFromNodeAndPersist(String sha256, String fromNode) {
    log.info("fetching a blob {} from {} ", sha256, fromNode);
    ByteBuffer[] result = new ByteBuffer[1];
    String url = coreContainer.getZkController().getZkStateReader().getBaseUrlForNodeName(fromNode);
    if (url == null) throw new SolrException(BAD_REQUEST, "No such node");
    coreContainer.getUpdateShardHandler().getUpdateExecutor().submit(() -> {
      String fromUrl = url.replace("/solr", "/api") + "/node/blob/" + sha256;
      try {
        HttpClient httpClient = coreContainer.getUpdateShardHandler().getDefaultHttpClient();
        result[0] = Utils.executeGET(httpClient, fromUrl, Utils.newBytesConsumer((int) MAX_PKG_SIZE));
        String actualSha256 = sha256Digest(result[0]);
        if (sha256.equals(actualSha256)) {
          persistToFile(result[0], sha256);
        } else {
          result[0] = null;
          log.error("expected sha256 : {} actual sha256: {} from blob downloaded from {} ", sha256, actualSha256, fromNode);
        }
      } catch (IOException e) {
        log.error("Unable to fetch jar: {} from node: {}", sha256, fromNode);
      }
    });
    return result[0];
  }


  @EndPoint(spec = "node.blob.GET",
  permission = PermissionNameProvider.Name.BLOB_READ)
  public class BlobRead {

    @Command
    public void get(CallInfo info){
      SolrQueryRequest req = info.req;
      SolrQueryResponse rsp = info.rsp;
      String sha256 = ((V2HttpCall) req.getHttpSolrCall()).getUrlParts().get("sha256");
      if (sha256 == null) {
        rsp.add("blob", fileList(req.getParams()));
      } else {
        if (!fetchBlobToFS(sha256)) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such blob");
        }

        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.add(CommonParams.WT, FILE_STREAM);
        req.setParams(SolrParams.wrapDefaults(solrParams, req.getParams()));
        rsp.add(FILE_STREAM, (SolrCore.RawWriter) os -> {
          ByteBuffer b = tmpBlobs.get(sha256);
          if (b != null) {
            os.write(b.array(), b.arrayOffset(), b.limit());
          } else {
            File file = new File(getBlobsPath().toFile(), sha256);
            try (FileInputStream is = new FileInputStream(file)) {
              org.apache.commons.io.IOUtils.copy(is, os);
            }
          }
        });
      }
    }

  }
}
