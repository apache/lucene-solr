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

package org.apache.solr.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.BlobRepository;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.CryptoKeys;
import org.apache.solr.util.SimplePostTool;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.handler.ReplicationHandler.FILE_STREAM;


public class PackageStoreAPI {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String PACKAGESTORE_DIRECTORY = "filestore";


  private final CoreContainer coreContainer;
  PackageStore packageStore;
  public final FSRead readAPI = new FSRead();
  public final FSWrite writeAPI = new FSWrite();

  public PackageStoreAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    packageStore = new DistribPackageStore(coreContainer);
  }

  public PackageStore getPackageStore() {
    return packageStore;
  }

  /**
   * get a list of nodes randomly shuffled
   * * @lucene.internal
   */
  public ArrayList<String> shuffledNodes() {
    Set<String> liveNodes = coreContainer.getZkController().getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList(liveNodes);
    l.remove(coreContainer.getZkController().getNodeName());
    Collections.shuffle(l, BlobRepository.RANDOM);
    return l;
  }

  public void validateFiles(List<String> files, boolean validateSignatures, Consumer<String> errs) {
    for (String path : files) {
      try {
        PackageStore.FileType type = packageStore.getType(path, true);
        if (type != PackageStore.FileType.FILE) {
          errs.accept("No such file : " + path);
          continue;
        }

        packageStore.get(path, entry -> {
          if (entry.getMetaData().signatures == null ||
              entry.getMetaData().signatures.isEmpty()) {
            errs.accept(path + " has no signature");
            return;
          }
          if (validateSignatures) {
            try {
              validate(entry.meta.signatures, entry);
            } catch (SolrException e) {
              log.error("error validating package artifact", e);
              errs.accept(e.getMessage());
            }
          }
        }, false);
      } catch (Exception e) {
        log.error("Error reading file ", e);
        errs.accept("Error reading file " + path + " " + e.getMessage());
      }
    }

  }

  @EndPoint(
      path = "/cluster/files/*",
      method = SolrRequest.METHOD.PUT,
      permission = PermissionNameProvider.Name.FILESTORE_WRITE_PERM)
  public class FSWrite {

    static final String TMP_ZK_NODE = "/packageStoreWriteInProgress";

    @Command
    public void upload(SolrQueryRequest req, SolrQueryResponse rsp) {
      try {
        coreContainer.getZkController().getZkClient().create(TMP_ZK_NODE, "true".getBytes(UTF_8),
            CreateMode.EPHEMERAL, true);

        Iterable<ContentStream> streams = req.getContentStreams();
        if (streams == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no payload");
        String path = req.getPathTemplateValues().get("*");
        if (path == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No path");
        }
        validateName(path);
        ContentStream stream = streams.iterator().next();
        try {
          ByteBuffer buf = SimplePostTool.inputStreamToByteArray(stream.getStream());
          String sha512 = DigestUtils.sha512Hex(new ByteBufferInputStream(buf));
          List<String> signatures = readSignatures(req, buf);
          Map<String, Object> vals = new HashMap<>();
          vals.put(MetaData.SHA512, sha512);
          if (signatures != null) {
            vals.put("sig", signatures);
          }
          PackageStore.FileType type = packageStore.getType(path, true);
          if(type != PackageStore.FileType.NOFILE) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,  "Path already exists "+ path);
          }
          packageStore.put(new PackageStore.FileEntry(buf, new MetaData(vals), path));
          rsp.add(CommonParams.FILE, path);
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }
      } catch (InterruptedException e) {
        log.error("Unexpected error", e);
      } catch (KeeperException.NodeExistsException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "A write is already in process , try later");
      } catch (KeeperException e) {
        log.error("Unexpected error", e);
      } finally {
        try {
          coreContainer.getZkController().getZkClient().delete(TMP_ZK_NODE, -1, true);
        } catch (Exception e) {
          log.error("Unexpected error  ", e);
        }
      }
    }

    private List<String> readSignatures(SolrQueryRequest req, ByteBuffer buf)
        throws SolrException, IOException {
      String[] signatures = req.getParams().getParams("sig");
      if (signatures == null || signatures.length == 0) return null;
      List<String> sigs = Arrays.asList(signatures);
      validate(sigs, buf);
      return sigs;
    }

    public void validate(List<String> sigs,
                         ByteBuffer buf) throws SolrException, IOException {
      Map<String, byte[]> keys = CloudUtil.getTrustedKeys(
          coreContainer.getZkController().getZkClient(), "exe");
      if (keys == null || keys.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "ZK does not have any keys");
      }
      CryptoKeys cryptoKeys = null;
      try {
        cryptoKeys = new CryptoKeys(keys);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error parsing public keys in ZooKeeper");
      }
      for (String sig : sigs) {
        if (cryptoKeys.verify(sig, buf) == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Signature does not match any public key : " + sig +" len: "+buf.limit()+  " content sha512: "+
              DigestUtils.sha512Hex(new ByteBufferInputStream(buf)));
        }

      }
    }

  }

  @EndPoint(
      path = "/node/files/*",
      method = SolrRequest.METHOD.GET,
      permission = PermissionNameProvider.Name.FILESTORE_READ_PERM)
  public class FSRead {
    @Command
    public void read(SolrQueryRequest req, SolrQueryResponse rsp) {
      String path = req.getPathTemplateValues().get("*");
      String pathCopy = path;
      String getFrom = req.getParams().get("getFrom");
      if (getFrom != null) {
        coreContainer.getUpdateShardHandler().getUpdateExecutor().submit(() -> {
          log.debug("Downloading file {}", pathCopy);
          try {
            packageStore.fetch(pathCopy, getFrom);
          } catch (Exception e) {
            log.error("Failed to download file: " + pathCopy, e);
          }
          log.info("downloaded file: {}", pathCopy);
        });
        return;

      }
      if (path == null) {
        path = "";
      }

      PackageStore.FileType typ = packageStore.getType(path, false);
      if (typ == PackageStore.FileType.NOFILE) {
        rsp.add("files", Collections.singletonMap(path, null));
        return;
      }
      if (typ == PackageStore.FileType.DIRECTORY) {
        rsp.add("files", Collections.singletonMap(path, packageStore.list(path, null)));
        return;
      }
      if (req.getParams().getBool("meta", false)) {
        if (typ == PackageStore.FileType.FILE) {
          int idx = path.lastIndexOf('/');
          String fileName = path.substring(idx + 1);
          String parentPath = path.substring(0, path.lastIndexOf('/'));
          List l = packageStore.list(parentPath, s -> s.equals(fileName));
          rsp.add("files", Collections.singletonMap(path, l.isEmpty() ? null : l.get(0)));
          return;
        }
      } else {
        writeRawFile(req, rsp, path);
      }
    }

    private void writeRawFile(SolrQueryRequest req, SolrQueryResponse rsp, String path) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add(CommonParams.WT, FILE_STREAM);
      req.setParams(SolrParams.wrapDefaults(solrParams, req.getParams()));
      rsp.add(FILE_STREAM, (SolrCore.RawWriter) os -> {
        packageStore.get(path, (it) -> {
          try {
            org.apache.commons.io.IOUtils.copy(it.getInputStream(), os);
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading file" + path);
          }
        }, false);

      });
    }

  }

  static class MetaData implements MapWriter {
    public static final String SHA512 = "sha512";
    String sha512;
    List<String> signatures;
    Map<String, Object> otherAttribs;

    public MetaData(Map m) {
      m = Utils.getDeepCopy(m, 3);
      this.sha512 = (String) m.remove(SHA512);
      this.signatures = (List<String>) m.remove("sig");
      this.otherAttribs = m;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.putIfNotNull("sha512", sha512);
      ew.putIfNotNull("sig", signatures);
      if (!otherAttribs.isEmpty()) {
        otherAttribs.forEach(ew.getBiConsumer());
      }
    }
  }

  static final String INVALIDCHARS = " /\\#&*\n\t%@~`=+^$><?{}[]|:;!";

  public static void validateName(String path) {
    if (path == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "empty path");
    }
    List<String> parts = StrUtils.splitSmart(path, '/', true);
    for (String part : parts) {
      if (part.charAt(0) == '.') {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "cannot start with period");
      }
      for (int i = 0; i < part.length(); i++) {
        for (int j = 0; j < INVALIDCHARS.length(); j++) {
          if (part.charAt(i) == INVALIDCHARS.charAt(j))
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported char in file name: " + part);
        }
      }
    }
  }

  public void validate(List<String> sigs,
                       PackageStore.FileEntry entry) throws SolrException {
    Map<String, byte[]> keys = CloudUtil.getTrustedKeys(
        coreContainer.getZkController().getZkClient(), "exe");
    if (keys == null || keys.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "ZooKeeper does not have any public keys");
    }
    CryptoKeys cryptoKeys = null;
    try {
      cryptoKeys = new CryptoKeys(keys);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error parsing public keys in ZooKeeper");
    }
    for (String sig : sigs) {
      Supplier<String> errMsg = () -> "Signature does not match any public key : " + sig + "sha256 "+ entry.getMetaData().sha512;
      if (entry.getBuffer() != null) {
        if (cryptoKeys.verify(sig, entry.getBuffer()) == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errMsg.get());
        }
      } else {
        InputStream inputStream = entry.getInputStream();
        if (cryptoKeys.verify(sig, inputStream) == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errMsg.get());
        }

      }

    }
  }
}
