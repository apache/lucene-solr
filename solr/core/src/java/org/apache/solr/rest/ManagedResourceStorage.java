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
package org.apache.solr.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * Abstract base class that provides most of the functionality needed
 * to store arbitrary data for managed resources. Concrete implementations
 * need to decide the underlying format that data is stored in, such as JSON.
 * 
 * The underlying storage I/O layer will be determined by the environment
 * Solr is running in, e.g. in cloud mode, data will be stored and loaded
 * from ZooKeeper.
 */
public abstract class ManagedResourceStorage {
  
  /**
   * Hides the underlying storage implementation for data being managed
   * by a ManagedResource. For instance, a ManagedResource may use JSON as
   * the data format and an instance of this class to persist and load 
   * the JSON bytes to/from some backing store, such as ZooKeeper.
   */
  public static interface StorageIO {
    String getInfo();
    void configure(SolrResourceLoader loader, NamedList<String> initArgs) throws SolrException;
    boolean exists(String storedResourceId) throws IOException;
    InputStream openInputStream(String storedResourceId) throws IOException;  
    OutputStream openOutputStream(String storedResourceId) throws IOException;
    boolean delete(String storedResourceId) throws IOException;
  }
  
  public static final String STORAGE_IO_CLASS_INIT_ARG = "storageIO"; 
  public static final String STORAGE_DIR_INIT_ARG = "storageDir";
  
  /**
   * Creates a new StorageIO instance for a Solr core, taking into account
   * whether the core is running in cloud mode as well as initArgs. 
   */
  public static StorageIO newStorageIO(String collection, SolrResourceLoader resourceLoader, NamedList<String> initArgs) {
    StorageIO storageIO;

    SolrZkClient zkClient = null;
    String zkConfigName = null;
    if (resourceLoader instanceof ZkSolrResourceLoader) {
      zkClient = ((ZkSolrResourceLoader)resourceLoader).getZkController().getZkClient();
      try {
        zkConfigName = ((ZkSolrResourceLoader)resourceLoader).getZkController().
            getZkStateReader().readConfigName(collection);
      } catch (Exception e) {
        log.error("Failed to get config name due to", e);
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Failed to load config name for collection:" + collection  + " due to: ", e);
      }
      if (zkConfigName == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, 
            "Could not find config name for collection:" + collection);
      }
    }
    
    if (initArgs.get(STORAGE_IO_CLASS_INIT_ARG) != null) {
      storageIO = resourceLoader.newInstance(initArgs.get(STORAGE_IO_CLASS_INIT_ARG), StorageIO.class); 
    } else {
      if (zkClient != null) {
        String znodeBase = "/configs/"+zkConfigName;
        log.debug("Setting up ZooKeeper-based storage for the RestManager with znodeBase: "+znodeBase);
        storageIO = new ManagedResourceStorage.ZooKeeperStorageIO(zkClient, znodeBase);
      } else {
        storageIO = new FileStorageIO();        
      }
    }
    
    if (storageIO instanceof FileStorageIO) {
      // using local fs, if storageDir is not set in the solrconfig.xml, assume the configDir for the core
      if (initArgs.get(STORAGE_DIR_INIT_ARG) == null) {
        File configDir = new File(resourceLoader.getConfigDir());
        boolean hasAccess = false;
        try {
          hasAccess = configDir.isDirectory() && configDir.canWrite();
        } catch (java.security.AccessControlException ace) {}
        
        if (hasAccess) {
          initArgs.add(STORAGE_DIR_INIT_ARG, configDir.getAbsolutePath());
        } else {
          // most likely this is because of a unit test 
          // that doesn't have write-access to the config dir
          // while this failover approach is not ideal, it's better
          // than causing the core to fail esp. if managed resources aren't being used
          log.warn("Cannot write to config directory "+configDir.getAbsolutePath()+
              "; switching to use InMemory storage instead.");
          storageIO = new ManagedResourceStorage.InMemoryStorageIO();
        }
      }       
    }
    
    storageIO.configure(resourceLoader, initArgs);     
    
    return storageIO;
  }
  
  /**
   * Local file-based storage implementation.
   */
  public static class FileStorageIO implements StorageIO {

    private String storageDir;
    
    @Override
    public void configure(SolrResourceLoader loader, NamedList<String> initArgs) throws SolrException {
      String storageDirArg = initArgs.get(STORAGE_DIR_INIT_ARG);
      
      if (storageDirArg == null || storageDirArg.trim().length() == 0)
        throw new IllegalArgumentException("Required configuration parameter '"+
           STORAGE_DIR_INIT_ARG+"' not provided!");
      
      File dir = new File(storageDirArg);
      if (!dir.isDirectory())
        dir.mkdirs();

      storageDir = dir.getAbsolutePath();      
      log.info("File-based storage initialized to use dir: "+storageDir);
    }
    
    @Override
    public boolean exists(String storedResourceId) throws IOException {
      return (new File(storageDir, storedResourceId)).exists();
    }    
    
    @Override
    public InputStream openInputStream(String storedResourceId) throws IOException {
      return new FileInputStream(storageDir+"/"+storedResourceId);
    }

    @Override
    public OutputStream openOutputStream(String storedResourceId) throws IOException {
      return new FileOutputStream(storageDir+"/"+storedResourceId);
    }

    @Override
    public boolean delete(String storedResourceId) throws IOException {
      File storedFile = new File(storageDir, storedResourceId);
      return deleteIfFile(storedFile);
    }
    
    // TODO: this interface should probably be changed, this simulates the old behavior,
    // only throw security exception, just return false otherwise
    private boolean deleteIfFile(File f) {
      if (!f.isFile()) {
        return false;
      }
      try {
        Files.delete(f.toPath());
        return true;
      } catch (IOException cause) {
        return false;
      }
    }

    @Override
    public String getInfo() {
      return "file:dir="+storageDir;
    }
  } // end FileStorageIO
  
  /**
   * ZooKeeper based storage implementation that uses the SolrZkClient provided
   * by the CoreContainer.
   */
  public static class ZooKeeperStorageIO implements StorageIO {
    
    protected SolrZkClient zkClient;
    protected String znodeBase;
    protected boolean retryOnConnLoss = true;
    
    public ZooKeeperStorageIO(SolrZkClient zkClient, String znodeBase) {
      this.zkClient = zkClient;  
      this.znodeBase = znodeBase;
    }

    @Override
    public void configure(SolrResourceLoader loader, NamedList<String> initArgs) throws SolrException {
      // validate connectivity and the configured znode base
      try {
        if (!zkClient.exists(znodeBase, retryOnConnLoss)) {
          zkClient.makePath(znodeBase, retryOnConnLoss);
        }
      } catch (Exception exc) {
        String errMsg = String.format
            (Locale.ROOT, "Failed to verify znode at %s due to: %s", znodeBase, exc.toString());
        log.error(errMsg, exc);
        throw new SolrException(ErrorCode.SERVER_ERROR, errMsg, exc);
      }
      
      log.info("Configured ZooKeeperStorageIO with znodeBase: "+znodeBase);      
    }    
    
    @Override
    public boolean exists(String storedResourceId) throws IOException {
      final String znodePath = getZnodeForResource(storedResourceId);
      try {
        return zkClient.exists(znodePath, retryOnConnLoss);
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException)e;
        } else {
          throw new IOException("Failed to read data at "+znodePath, e);
        }
      }
    }    
    
    @Override
    public InputStream openInputStream(String storedResourceId) throws IOException {
      final String znodePath = getZnodeForResource(storedResourceId);
      byte[] znodeData = null;
      try {
        if (zkClient.exists(znodePath, retryOnConnLoss)) {
          znodeData = zkClient.getData(znodePath, null, null, retryOnConnLoss);
        }
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException)e;
        } else {
          throw new IOException("Failed to read data at "+znodePath, e);
        }
      }
      
      if (znodeData != null) {
        log.debug("Read {} bytes from znode {}", znodeData.length, znodePath);
      } else {
        znodeData = new byte[0];
        log.debug("No data found for znode {}", znodePath);
      }
      
      return new ByteArrayInputStream(znodeData);
    }

    @Override
    public OutputStream openOutputStream(String storedResourceId) throws IOException {
      final String znodePath = getZnodeForResource(storedResourceId);
      final boolean retryOnConnLoss = this.retryOnConnLoss;
      ByteArrayOutputStream baos = new ByteArrayOutputStream() {
        @Override
        public void close() {
          byte[] znodeData = toByteArray();
          try {
            if (zkClient.exists(znodePath, retryOnConnLoss)) {
              zkClient.setData(znodePath, znodeData, retryOnConnLoss);
              log.info("Wrote {} bytes to existing znode {}", znodeData.length, znodePath);
            } else {
              zkClient.makePath(znodePath, znodeData, retryOnConnLoss);
              log.info("Wrote {} bytes to new znode {}", znodeData.length, znodePath);
            }
          } catch (Exception e) {
            // have to throw a runtimer here as we're in close, 
            // which doesn't throw IOException
            if (e instanceof RuntimeException) {
              throw (RuntimeException)e;              
            } else {
              throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
                  "Failed to save data to ZooKeeper znode: "+znodePath+" due to: "+e, e);
            }
          }
        }
      };
      return baos;
    }

    /**
     * Returns the Znode for the given storedResourceId by combining it
     * with the znode base.
     */
    protected String getZnodeForResource(String storedResourceId) {
      return String.format(Locale.ROOT, "%s/%s", znodeBase, storedResourceId);
    }

    @Override
    public boolean delete(String storedResourceId) throws IOException {
      boolean wasDeleted = false;
      final String znodePath = getZnodeForResource(storedResourceId);
      
      // this might be overkill for a delete operation
      try {
        if (zkClient.exists(znodePath, retryOnConnLoss)) {
          log.debug("Attempting to delete znode {}", znodePath);
          zkClient.delete(znodePath, -1, retryOnConnLoss);
          wasDeleted = zkClient.exists(znodePath, retryOnConnLoss);
          
          if (wasDeleted) {
            log.info("Deleted znode {}", znodePath);
          } else {
            log.warn("Failed to delete znode {}", znodePath);
          }
        } else {
          log.warn("Znode {} does not exist; delete operation ignored.", znodePath);
        }
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException)e;
        } else {
          throw new IOException("Failed to read data at "+znodePath, e);
        }
      }
      
      return wasDeleted;
    }

    @Override
    public String getInfo() {
      return "ZooKeeperStorageIO:path="+znodeBase;
    }
  } // end ZooKeeperStorageIO
  
  /**
   * Memory-backed storage IO; not really intended for storage large amounts
   * of data in production, but useful for testing and other transient workloads.
   */
  public static class InMemoryStorageIO implements StorageIO {
    
    Map<String,BytesRef> storage = new HashMap<>();
    
    @Override
    public void configure(SolrResourceLoader loader, NamedList<String> initArgs)
        throws SolrException {}

    @Override
    public boolean exists(String storedResourceId) throws IOException {
      return storage.containsKey(storedResourceId);
    }
    
    @Override
    public InputStream openInputStream(String storedResourceId)
        throws IOException {
      
      BytesRef storedVal = storage.get(storedResourceId);
      if (storedVal == null)
        throw new FileNotFoundException(storedResourceId);
      
      return new ByteArrayInputStream(storedVal.bytes, storedVal.offset, storedVal.length);            
    }

    @Override
    public OutputStream openOutputStream(final String storedResourceId)
        throws IOException {
      ByteArrayOutputStream boas = new ByteArrayOutputStream() {
        @Override
        public void close() {
          storage.put(storedResourceId, new BytesRef(toByteArray()));
        }
      };
      return boas;
    }

    @Override
    public boolean delete(String storedResourceId) throws IOException {
      return (storage.remove(storedResourceId) != null);
    }

    @Override
    public String getInfo() {
      return "InMemoryStorage";
    }
  } // end InMemoryStorageIO class 
  
  /**
   * Default storage implementation that uses JSON as the storage format for managed data.
   */
  public static class JsonStorage extends ManagedResourceStorage {
    
    public JsonStorage(StorageIO storageIO, SolrResourceLoader loader) {
      super(storageIO, loader);
    }

    /**
     * Determines the relative path (from the storage root) for the given resource.
     * In this case, it returns a file named with the .json extension.
     */
    @Override
    public String getStoredResourceId(String resourceId) {
      return resourceId.replace('/','_')+".json";
    }  
      
    @Override
    protected Object parseText(Reader reader, String resourceId) throws IOException {
      return Utils.fromJSON(reader);
    }

    @Override
    public void store(String resourceId, Object toStore) throws IOException {
      String json = toJSONString(toStore);
      String storedResourceId = getStoredResourceId(resourceId);
      OutputStreamWriter writer = null;
      try {
        writer = new OutputStreamWriter(storageIO.openOutputStream(storedResourceId), UTF_8);
        writer.write(json);
        writer.flush();
      } finally {
        if (writer != null) {
          try {
            writer.close();
          } catch (Exception ignore){}
        }
      }    
      log.info("Saved JSON object to path {} using {}", 
          storedResourceId, storageIO.getInfo());
    }
  } // end JsonStorage 
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final Charset UTF_8 = StandardCharsets.UTF_8;
  
  protected StorageIO storageIO;
  protected SolrResourceLoader loader;
    
  protected ManagedResourceStorage(StorageIO storageIO, SolrResourceLoader loader) {
    this.storageIO = storageIO;
    this.loader = loader;
  }

  /** Returns the resource loader used by this storage instance */
  public SolrResourceLoader getResourceLoader() {
    return loader;
  }

  /** Returns the storageIO instance used by this storage instance */
  public StorageIO getStorageIO() {
    return storageIO;
  }
  
  /**
   * Gets the unique identifier for a stored resource, typically based
   * on the resourceId and some storage-specific information, such as
   * file extension and storage root directory.
   */
  public abstract String getStoredResourceId(String resourceId);
   
  /**
   * Loads a resource from storage; the default implementation makes
   * the assumption that the data is stored as UTF-8 encoded text, 
   * such as JSON. This method should be overridden if that assumption
   * is invalid. 
   */
  public Object load(String resourceId) throws IOException {
    String storedResourceId = getStoredResourceId(resourceId);
    
    log.debug("Reading {} using {}", storedResourceId, storageIO.getInfo());
    
    InputStream inputStream = storageIO.openInputStream(storedResourceId);
    if (inputStream == null) {
      return null;
    }
    Object parsed;
    try (InputStreamReader reader = new InputStreamReader(inputStream, UTF_8)) {
      parsed = parseText(reader, resourceId);
    }
    
    String objectType = (parsed != null) ? parsed.getClass().getSimpleName() : "null"; 
    log.info(String.format(Locale.ROOT, "Loaded %s at path %s using %s",
                                        objectType, storedResourceId, storageIO.getInfo()));
    
    return parsed;
  }

  /**
   * Called by {@link ManagedResourceStorage#load(String)} to convert the
   * serialized resource into its in-memory representation.
   */
  protected Object parseText(Reader reader, String resourceId) throws IOException {
    // no-op: base classes should override this if they deal with text.
    return null;
  }

  /** Persists the given toStore object with the given resourceId. */
  public abstract void store(String resourceId, Object toStore) throws IOException;

  /** Removes the given resourceId's persisted representation. */
  public boolean delete(String resourceId) throws IOException {
    return storageIO.delete(getStoredResourceId(resourceId));
  }
}
