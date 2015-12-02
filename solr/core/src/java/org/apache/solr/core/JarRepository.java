package org.apache.solr.core;

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

import static org.apache.solr.common.SolrException.ErrorCode.SERVICE_UNAVAILABLE;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.util.CryptoKeys;
import org.apache.solr.util.SimplePostTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this class is to store the Jars loaded in memory and to keep only one copy of the Jar in a single node.
 */
public class JarRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final Random RANDOM;

  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  private final CoreContainer coreContainer;
  private Map<String, JarContent> jars = new ConcurrentHashMap<>();

  public JarRepository(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  /**
   * Returns the contents of a jar and increments a reference count. Please return the same object to decrease the refcount
   *
   * @param key it is a combination of blobname and version like blobName/version
   * @return The reference of a jar
   */
  public JarContentRef getJarIncRef(String key) {
    JarContent jar = jars.get(key);
    if (jar == null) {
      if (this.coreContainer.isZooKeeperAware()) {
        Replica replica = getSystemCollReplica();
        String url = replica.getStr(BASE_URL_PROP) + "/.system/blob/" + key + "?wt=filestream";

        HttpClient httpClient = coreContainer.getUpdateShardHandler().getHttpClient();
        HttpGet httpGet = new HttpGet(url);
        ByteBuffer b;
        try {
          HttpResponse entity = httpClient.execute(httpGet);
          int statusCode = entity.getStatusLine().getStatusCode();
          if (statusCode != 200) {
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "no such blob or version available: " + key);
          }
          b = SimplePostTool.inputStreamToByteArray(entity.getEntity().getContent());
        } catch (Exception e) {
          if (e instanceof SolrException) {
            throw (SolrException) e;
          } else {
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "could not load : " + key, e);
          }
        } finally {
          httpGet.releaseConnection();
        }
        jars.put(key, jar = new JarContent(key, b));
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Jar loading is not supported in non-cloud mode");
        // todo
      }

    }

    JarContentRef ref = new JarContentRef(jar);
    synchronized (jar.references) {
      jar.references.add(ref);
    }
    return ref;

  }

  private Replica getSystemCollReplica() {
    ZkStateReader zkStateReader = this.coreContainer.getZkController().getZkStateReader();
    ClusterState cs = zkStateReader.getClusterState();
    DocCollection coll = cs.getCollectionOrNull(CollectionsHandler.SYSTEM_COLL);
    if (coll == null) throw new SolrException(SERVICE_UNAVAILABLE, ".system collection not available");
    ArrayList<Slice> slices = new ArrayList<>(coll.getActiveSlices());
    if (slices.isEmpty()) throw new SolrException(SERVICE_UNAVAILABLE, "No active slices for .system collection");
    Collections.shuffle(slices, RANDOM); //do load balancing

    Replica replica = null;
    for (Slice slice : slices) {
      List<Replica> replicas = new ArrayList<>(slice.getReplicasMap().values());
      Collections.shuffle(replicas, RANDOM);
      for (Replica r : replicas) {
        if (r.getState() == Replica.State.ACTIVE) {
          if(zkStateReader.getClusterState().getLiveNodes().contains(r.get(ZkStateReader.NODE_NAME_PROP))){
            replica = r;
            break;
          } else {
            log.info("replica {} says it is active but not a member of live nodes", r.get(ZkStateReader.NODE_NAME_PROP));
          }
        }
      }
    }
    if (replica == null) {
      throw new SolrException(SERVICE_UNAVAILABLE, ".no active replica available for .system collection");
    }
    return replica;
  }

  /**
   * This is to decrement a ref count
   *
   * @param ref The reference that is already there. Doing multiple calls with same ref will not matter
   */
  public void decrementJarRefCount(JarContentRef ref) {
    if (ref == null) return;
    synchronized (ref.jar.references) {
      if (!ref.jar.references.remove(ref)) {
        log.error("Multiple releases for the same reference");
      }
      if (ref.jar.references.isEmpty()) {
        jars.remove(ref.jar.key);
      }
    }

  }

  public static class JarContent {
    private final String key;
    // TODO move this off-heap
    private final ByteBuffer buffer;
    // ref counting mechanism
    private final Set<JarContentRef> references = new HashSet<>();

    public JarContent(String key, ByteBuffer buffer) {
      this.key = key;
      this.buffer = buffer;
    }

    public ByteBuffer getFileContent(String entryName) throws IOException {
      ByteArrayInputStream zipContents = new ByteArrayInputStream(buffer.array(), buffer.arrayOffset(), buffer.limit());
      ZipInputStream zis = new ZipInputStream(zipContents);
      try {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          if (entryName == null || entryName.equals(entry.getName())) {
            SimplePostTool.BAOS out = new SimplePostTool.BAOS();
            byte[] buffer = new byte[2048];
            int size;
            while ((size = zis.read(buffer, 0, buffer.length)) != -1) {
              out.write(buffer, 0, size);
            }
            out.close();
            return out.getByteBuffer();
          }
        }
      } finally {
        zis.closeEntry();
      }
      return null;
    }

    public String checkSignature(String base64Sig, CryptoKeys keys) {
      return keys.verify(base64Sig, buffer);
    }

  }

  public static class JarContentRef {
    public final JarContent jar;

    private JarContentRef(JarContent jar) {
      this.jar = jar;
    }
  }

}
