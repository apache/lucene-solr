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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullAdlsProvider implements AdlsProvider {

  ADLStoreClient adlsClient;
  LoadingCache<String,DirectoryEntry> directoryEntryCache;
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public FullAdlsProvider(ADLStoreClient client){
    this.adlsClient=client;
    directoryEntryCache = Caffeine.newBuilder()
        .maximumSize(1000)
        .removalListener((key,value,cause)->{LOG.info("Expire file "+key+" because "+cause);})
        .expireAfterAccess(24, TimeUnit.HOURS)
        .build((key)->{
          LOG.info("Get file "+key+" info from ADLS");
          return getEntryQuietly((String)key);});
  }

  @Override
  public boolean rename(String path1, String path2, boolean overwrite) throws IOException {
    directoryEntryCache.invalidate(path1);
    boolean rename=adlsClient.rename(path1,path2,overwrite);

    LOG.info("rename "+path1+" to "+path2+" "+rename);

    directoryEntryCache.put(path2,getEntryQuietly(path2));

    return rename;
  }

  @Override
  public boolean checkExists(String path) throws  IOException{
    boolean exists= adlsClient.checkExists(path);
    if (!exists){
      directoryEntryCache.invalidate(path);
    }

    return exists;
  }

  @Override
  public boolean createDirectory(String path) throws  IOException{
    return adlsClient.createDirectory(path);
  }

  @Override
  public boolean delete(String path) throws  IOException{
    directoryEntryCache.invalidate(path);
    return adlsClient.delete(path);
  }

  @Override
  public List<DirectoryEntry> enumerateDirectory(String path) throws  IOException{
    return adlsClient.enumerateDirectory(path);
  }

  @Override
  public DirectoryEntry getDirectoryEntry(String path) throws  IOException{
    return directoryEntryCache.get(path);
  }

  @Override
  public WrappedADLFileOutputStream createFile(String path, IfExists mode) throws  IOException{
    return new WrappedADLFileOutputStream(adlsClient.createFile(path,mode),
        path,
        (item)->LOG.info("new file, "+ path+" add to cache"),
        (item)-> this.directoryEntryCache.put(path,getEntryQuietly(item))
        );
  }

  private DirectoryEntry getEntryQuietly(String path){
    try {
      LOG.info("get entry for "+path);

      return adlsClient.getDirectoryEntry(path);
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  @Override
  public ADLFileInputStream getReadStream(String path) throws  IOException{
    return adlsClient.getReadStream(path);
  }

  @Override
  public boolean deleteRecursive(String path) throws  IOException {
    directoryEntryCache.asMap().keySet().stream()
        .filter((entryKey)->entryKey.startsWith(path))
        .forEach((entryKey)->directoryEntryCache.invalidate(entryKey));
    return adlsClient.deleteRecursive(path);
  }

  @Override
  public WrappedADLFileOutputStream getAppendStream(String path) throws  IOException {
    return new WrappedADLFileOutputStream(adlsClient.getAppendStream(path),path,
        (item)->directoryEntryCache.invalidate(item),(item)->LOG.info("invalidate due to APPEND"));
  }




}
