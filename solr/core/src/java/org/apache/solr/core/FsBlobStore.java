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
    String id = params.get(CommonParams.ID);
    File dir = getBlobsPath().toFile();

    String fromNode = params.get("fromNode");
    if (id != null && fromNode != null) {
      //asking to fetch it from somewhere if it does not exist locally
      if (!new File(dir, id).exists()) {
        if ("*".equals(fromNode)) {
          //asking to fetch from a random node
          fetchFromOtherNodes(id);
          return EMPTY;
        } else { // asking to fetch from a specific node
          fetchBlobFromNodeAndPersist(id, fromNode);
          return MapWriter.EMPTY;
        }
      }
    }
    return ew -> dir.listFiles((f, name) -> {
      if (id == null || name.equals(id)) {
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

  private ByteBuffer fetchFromOtherNodes(String id) {
    ByteBuffer[] result = new ByteBuffer[1];
    ArrayList<String> l = shuffledNodes();
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.ID, id);
    ZkStateReader stateReader = coreContainer.getZkController().getZkStateReader();
    for (String liveNode : l) {
      try {
        String baseurl = stateReader.getBaseUrlForNodeName(liveNode);
        String url = baseurl.replace("/solr", "/api");
        String reqUrl = url + "/node/blob?wt=javabin&omitHeader=true&id=" + id;
        boolean nodeHasBlob = false;
        Object nl = Utils.executeGET(coreContainer.getUpdateShardHandler().getDefaultHttpClient(), reqUrl, Utils.JAVABINCONSUMER);
        if (Utils.getObjectByPath(nl, false, Arrays.asList("blob", id)) != null) {
          nodeHasBlob = true;
        }

        if (nodeHasBlob) {
          result[0] = fetchBlobFromNodeAndPersist(id, liveNode);
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

  public static class BlobName {
    final String sha256;
    final String fname;

    BlobName(String name) {
      int idx = name.indexOf('-');
      if (idx == -1) {
        sha256 = name;
        fname = null;
        return;
      } else {
        sha256 = name.substring(0, idx);
        fname = name.substring(idx + 1);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof BlobName) {
        BlobName that = (BlobName) obj;
        return Objects.equals(this.sha256, that.sha256) && Objects.equals(this.fname, that.fname);


      }
      return false;
    }

    public String name() {
      return fname == null ? sha256 : sha256 + "-" + fname;
    }

    @Override
    public String toString() {
      return name();
    }
  }


  private void persistToFile(ByteBuffer b, String id) throws IOException {
    BlobName blobName = new BlobName(id);
    String actual = sha256Digest(b);
    if (!Objects.equals(actual, blobName.sha256)) {
      throw new SolrException(SERVER_ERROR, "invalid id for blob actual: " + actual + " expected : " + blobName.sha256);
    }
    File file = new File(getBlobsPath().toFile(), id);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(b.array(), 0, b.limit());
    }
    log.info("persisted a blob {} ", id);
    IOUtils.fsync(file.toPath(), false);
  }


  boolean fetchBlobToFS(String id) {
    File f = new File(getBlobsPath().toFile(), id);
    if (f.exists()) return true;
    fetchFromOtherNodes(id);
    return f.exists();
  }

  /**
   * Read a blob from the blobstore file system
   */
  public void readBlob(String id, Consumer<InputStream> consumer) throws IOException {
    if (!fetchBlobToFS(id)) throw new FileNotFoundException("No such blob: " + id);
    File f = new File(getBlobsPath().toFile(), id);
    try (InputStream is = new FileInputStream(f)) {
      consumer.accept(is);
    }
  }

  /**
   * This distributes a blob to all nodes in the cluster
   * *USE CAREFULLY*
   */
  public void distributeBlob(ByteBuffer buf, String id) throws IOException {
    persistToFile(buf, id);
    tmpBlobs.put(id, buf);
    List<String> nodes = coreContainer.getBlobStore().shuffledNodes();
    int i = 0;
    int FETCHFROM_SRC = 50;
    try {
      for (String node : nodes) {
        String baseUrl = coreContainer.getZkController().getZkStateReader().getBaseUrlForNodeName(node);
        String url = baseUrl.replace("/solr", "/api") + "/node/blob?id=" + id + "&fromNode=";
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
          coreContainer.getBlobStore().tmpBlobs.remove(id);
        }
      }).start();


    }

  }


  private ByteBuffer fetchBlobFromNodeAndPersist(String id, String fromNode) {
    log.info("fetching a blob {} from {} ", id, fromNode);
    ByteBuffer[] result = new ByteBuffer[1];
    String url = coreContainer.getZkController().getZkStateReader().getBaseUrlForNodeName(fromNode);
    if (url == null) throw new SolrException(BAD_REQUEST, "No such node");
    coreContainer.getUpdateShardHandler().getUpdateExecutor().submit(() -> {
      String fromUrl = url.replace("/solr", "/api") + "/node/blob/" + id;
      try {
        HttpClient httpClient = coreContainer.getUpdateShardHandler().getDefaultHttpClient();
        result[0] = Utils.executeGET(httpClient, fromUrl, Utils.newBytesConsumer((int) MAX_PKG_SIZE));
        String actualSha256 = sha256Digest(result[0]);
        BlobName blobName = new BlobName(id);
        if (blobName.sha256.equals(actualSha256)) {
          persistToFile(result[0], id);
        } else {
          result[0] = null;
          log.error("expected sha256 : {} actual sha256: {} from blob downloaded from {} ", blobName.sha256, actualSha256, fromNode);
        }
      } catch (IOException e) {
        log.error("Unable to fetch jar: {} from node: {}", id, fromNode);
      }
    });
    return result[0];
  }


  @EndPoint(spec = "node.blob.GET",
      permission = PermissionNameProvider.Name.BLOB_READ)
  public class BlobRead {

    @Command
    public void get(CallInfo info) {
      SolrQueryRequest req = info.req;
      SolrQueryResponse rsp = info.rsp;
      String id = ((V2HttpCall) req.getHttpSolrCall()).getUrlParts().get(CommonParams.ID);
      if (id == null) {
        rsp.add("blob", fileList(req.getParams()));
      } else {
        if (!fetchBlobToFS(id)) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such blob");
        }

        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.add(CommonParams.WT, FILE_STREAM);
        req.setParams(SolrParams.wrapDefaults(solrParams, req.getParams()));
        rsp.add(FILE_STREAM, (SolrCore.RawWriter) os -> {
          ByteBuffer b = tmpBlobs.get(id);
          if (b != null) {
            os.write(b.array(), b.arrayOffset(), b.limit());
          } else {
            File file = new File(getBlobsPath().toFile(), id);
            try (FileInputStream is = new FileInputStream(file)) {
              org.apache.commons.io.IOUtils.copy(is, os);
            }
          }
        });
      }
    }

  }
}
