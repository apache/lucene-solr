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
package org.apache.solr.store.adls;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.core.AdlsDirectoryFactory;
import org.apache.solr.store.blockcache.CustomBufferedIndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdlsDirectory extends BaseDirectory {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final int DEFAULT_BUFFER_SIZE = 4096;
  
  private static final String LF_EXT = ".lf";
  protected final String adlsDirPath;
  private AdlsProvider client;

  private final int bufferSize;

  public AdlsDirectory(String adlsDirPath, AdlsProvider client) throws IOException {
    this(adlsDirPath, AdlsLockFactory.INSTANCE, client, DEFAULT_BUFFER_SIZE);
  }

  public AdlsDirectory(String adlsDirPath, AdlsProvider client,int bufferSize) throws IOException {
    this(adlsDirPath, AdlsLockFactory.INSTANCE, client, bufferSize);
  }


  public AdlsDirectory(String adlsDirPath, LockFactory lockFactory, AdlsProvider client, int bufferSize)
      throws IOException {
    super(lockFactory);
    this.adlsDirPath = adlsDirPath;
    this.bufferSize = bufferSize;
    this.client=client;

    try {
      if (!client.checkExists(adlsDirPath)) {
        boolean success = client.createDirectory(adlsDirPath);
        if (!success) {
          throw new RuntimeException("Could not create directory: " + adlsDirPath);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Problem creating directory: " + adlsDirPath, e);
    }
  }
  
  @Override
  public void close() throws IOException {
    // The ADLS client doesn't have state so we don't have to worry.
    this.isOpen=false;
  }

  /**
   * Check whether this directory is open or closed. This check may return stale results in the form of false negatives.
   * @return true if the directory is definitely closed, false if the directory is open or is pending closure
   */
  public boolean isClosed() {
    return !isOpen;
  }


  /** Creates a new, empty file in the directory with the given name.
   Returns a stream writing this file. */
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return new AdlsFileWriter(this.client, adlsDirPath+'/'+name, name);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    String file = prefix+"-"+ UUID.randomUUID().toString()+suffix;
    return new AdlsFileWriter(this.client,file,file);
  }
  
  private String[] getNormalNames(List<String> files) {
    int size = files.size();
    for (int i = 0; i < size; i++) {
      String str = files.get(i);
      files.set(i, toNormalName(str));
    }
    return files.toArray(new String[] {});
  }
  
  private String toNormalName(String name) {
    if (name.endsWith(LF_EXT)) {
      return name.substring(0, name.length() - 3);
    }
    return name;
  }
  
  @Override
  public IndexInput openInput(String name, IOContext context)
      throws IOException {
    String mydir = adlsDirPath+"/"+AdlsDirectoryFactory.fixBadPath(name);
    if (!client.checkExists(mydir)){
      throw new FileNotFoundException("can't find "+mydir);
    }

    return new AdlsIndexInput(name, this.client, mydir, bufferSize);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    String path = adlsDirPath+"/"+name;
    LOG.info("Deleting {}", path);
    client.delete(path);
  }
  
  @Override
  public void rename(String source, String dest) throws IOException {
    String sourcePath = this.adlsDirPath+"/"+source;
    String destPath = this.adlsDirPath+"/"+dest;
    client.rename(sourcePath,destPath,false);
  }

  @Override
  public void syncMetaData() throws IOException {
    // TODO: how?
  }

  private DirectoryEntry getStatusObject(String path) {
    try {
      //,(key)->getStatusObject(destPath)
      return client.getDirectoryEntry(path);
    } catch (Exception e){
      throw new RuntimeException(e);
    }

  }

  @Override
  public long fileLength(String name) throws IOException {
    String en=this.adlsDirPath+"/"+name;
    return client.getDirectoryEntry(en).length;
  }

  public long fileModified(String name) throws IOException {
    String en = this.adlsDirPath+"/"+name;
    return client.getDirectoryEntry(en).lastModifiedTime.getTime();
  }
  
  @Override
  public String[] listAll() throws IOException {
    List<String> names = client.enumerateDirectory(adlsDirPath).stream()
        .map((dirEntry) -> dirEntry.name)
        .collect(Collectors.toList());

    return getNormalNames(names);
  }

  public String getAdlsDirPath() {
    return adlsDirPath;
  }
  
  public static class AdlsIndexInput extends CustomBufferedIndexInput {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    
    private final String path;
    private final ADLFileInputStream inputStream;
    private final long length;
    private boolean clone = false;
    
    public AdlsIndexInput(String name, AdlsProvider client, String path,
        int bufferSize) throws IOException {
      super(name, bufferSize);
      this.path = path;
      LOG.info("Opening normal index input on {}", path);

      this.length = client.getDirectoryEntry(path).length;
      this.inputStream=client.getReadStream(path);
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length)
        throws IOException {
      inputStream.seek(getFilePointer());
      IOUtils.readFully(this.inputStream,b,offset,length);
    }
    
    @Override
    protected void seekInternal(long pos) throws IOException {
      //inputStream.seek(getFilePointer());
    }
    
    @Override
    protected void closeInternal() throws IOException {
      LOG.info("Closing normal index input on {}", path);
      if (!clone) {
        inputStream.close();
      }
    }
    
    @Override
    public long length() {
      return length;
    }
    
    @Override
    public IndexInput clone() {
      AdlsIndexInput clone = (AdlsIndexInput) super.clone();
      clone.clone = true;
      return clone;
    }
  }
  
  @Override
  public void sync(Collection<String> names) throws IOException {
    LOG.debug("Sync called on {}", Arrays.toString(names.toArray()));
  }
  
  @Override
  public int hashCode() {
    return adlsDirPath.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof AdlsDirectory)) {
      return false;
    }
    return this.adlsDirPath.equals(((AdlsDirectory) obj).adlsDirPath);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "@" + adlsDirPath + " lockFactory=" + lockFactory;
  }

  public AdlsProvider getClient() {
    return client;
  }

}
