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

import static org.apache.solr.common.SolrException.ErrorCode.SERVICE_UNAVAILABLE;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this class is to store the Jars loaded in memory and to keep only one copy of the Jar in a single node.
 */
public class BlobRepository {
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
  private Map<String, BlobContent> blobs = new ConcurrentHashMap<>();

  public BlobRepository(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  public static ByteBuffer getFileContent(BlobContent blobContent, String entryName) throws IOException {
    ByteArrayInputStream zipContents = new ByteArrayInputStream(blobContent.buffer.array(), blobContent.buffer.arrayOffset(), blobContent.buffer.limit());
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

  /**
   * Returns the contents of a jar and increments a reference count. Please return the same object to decrease the refcount
   *
   * @param key it is a combination of blobname and version like blobName/version
   * @return The reference of a jar
   */
  public BlobContentRef getBlobIncRef(String key) {
    BlobContent aBlob = blobs.get(key);
    if (aBlob == null) {
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
        blobs.put(key, aBlob = new BlobContent(key, b));
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Jar loading is not supported in non-cloud mode");
        // todo
      }

    }

    BlobContentRef ref = new BlobContentRef(aBlob);
    synchronized (aBlob.references) {
      aBlob.references.add(ref);
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
  public void decrementBlobRefCount(BlobContentRef ref) {
    if (ref == null) return;
    synchronized (ref.blob.references) {
      if (!ref.blob.references.remove(ref)) {
        log.error("Multiple releases for the same reference");
      }
      if (ref.blob.references.isEmpty()) {
        blobs.remove(ref.blob.key);
      }
    }

  }

  public static class BlobContent {
    private final String key;
    private Map<String, Object> decodedObjects = null;
    // TODO move this off-heap
    private final ByteBuffer buffer;
    // ref counting mechanism
    private final Set<BlobContentRef> references = new HashSet<>();


    public BlobContent(String key, ByteBuffer buffer) {
      this.key = key;
      this.buffer = buffer;
    }

    /**
     * This method decodes the byte[] to a custom Object
     *
     * @param key     The key is used to store the decoded Object. it is possible to have multiple
     *                decoders for the same blob (may be unusual).
     * @param decoder A decoder instance
     * @return the decoded Object . If it was already decoded, then return from the cache
     */
    public <T> T decodeAndCache(String key, Decoder<T> decoder) {
      if (decodedObjects == null) {
        synchronized (this) {
          if (decodedObjects == null) decodedObjects = new ConcurrentHashMap<>();
        }
      }

      Object t = decodedObjects.get(key);
      if (t != null) return (T) t;
      t = decoder.decode(new ByteBufferInputStream(buffer));
      decodedObjects.put(key, t);
      return (T) t;

    }

    public String checkSignature(String base64Sig, CryptoKeys keys) {
      return keys.verify(base64Sig, buffer);
    }

  }

  public interface Decoder<T> {

    T decode(InputStream inputStream);
  }

  public static class BlobContentRef {
    public final BlobContent blob;

    private BlobContentRef(BlobContent blob) {
      this.blob = blob;
    }
  }

}
