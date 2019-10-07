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
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.SimplePostTool;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.ReplicationHandler.FILE_STREAM;


public class FileStoreAPI {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String FILESTORE_DIRECTORY = "filestore";


  private final CoreContainer coreContainer;
  FileStore fileStore;
  public final FSRead readAPI = new FSRead();
  public final FSWrite writeAPI = new FSWrite();

  public FileStoreAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    fileStore = new DistribFileStore(coreContainer);
  }

  public FileStore getFileStore() {
    return fileStore;
  }

  @EndPoint(
      path = "/cluster/filestore/*",
      method = SolrRequest.METHOD.POST,
      permission = PermissionNameProvider.Name.FILESTORE_WRITE_PERM)
  public class FSWrite {

    @Command
    public void upload(SolrQueryRequest req, SolrQueryResponse rsp) {
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
        fileStore.put(path, new MetaData(sha512), buf);
        rsp.add(CommonParams.FILE, path);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

  }

  @EndPoint(
      path = "/node/filestore/*",
      method = SolrRequest.METHOD.GET,
      permission = PermissionNameProvider.Name.FILESTORE_READ_PERM)
  public class FSRead {
    @Command
    public void read(SolrQueryRequest req, SolrQueryResponse rsp) {
      String path = req.getPathTemplateValues().get("*");
      String pathCopy = path;
      String getFrom = req.getParams().get("getFrom");
      if (getFrom != null) {
        new Thread(() -> {
          log.debug("Downloading file {}", pathCopy);
          fileStore.fetch(pathCopy, getFrom);
          log.info("downloaded file : {}", pathCopy);
        }).start();
        return;

      }
      if (path == null) {
        path = "";
      }

      FileStore.FileType typ = fileStore.getType(path);
      if (typ == FileStore.FileType.NOFILE) {
        rsp.add("files", Collections.singletonMap(path, null));
        return;
      }
      if (typ == FileStore.FileType.DIRECTORY) {
        rsp.add("files", Collections.singletonMap(path, fileStore.list(path, null)));
        return;
      }
      if (req.getParams().getBool("meta", false)) {
        if (typ == FileStore.FileType.FILE ) {
          int idx = path.lastIndexOf('/');
          String fileName = path.substring(idx+1);
          String parentPath = path.substring(0, path.lastIndexOf('/'));
          List l = fileStore.list(parentPath, s -> s.equals(fileName));
          rsp.add("files", Collections.singletonMap(path, l.isEmpty() ? null: l.get(0)));
          return;
        }
      } else {
        writeRawFile(req,rsp, path);
      }
    }

    private void writeRawFile(SolrQueryRequest req,SolrQueryResponse rsp, String path) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add(CommonParams.WT, FILE_STREAM);
      req.setParams(SolrParams.wrapDefaults(solrParams, req.getParams()));
      rsp.add(FILE_STREAM, (SolrCore.RawWriter) os -> {
        fileStore.get(path, (is, metaData) -> {
          try {
            org.apache.commons.io.IOUtils.copy(is, os);
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error reading file" + path);
          }
        });

      });
    }

  }

  static class MetaData implements MapWriter {
    String sha512;
    Map<String, String> signatures;
    Map<String, Object> otherAttribs;

    public MetaData(Map m) {
      m = Utils.getDeepCopy(m, 3);
      this.sha512 = (String) m.remove("sha512");
      this.signatures = (Map<String, String>) m.remove("sig");
      this.otherAttribs = m;
    }

    public MetaData(String sha512) {
      this.sha512 = sha512;
      otherAttribs = new LinkedHashMap<>();
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
}
