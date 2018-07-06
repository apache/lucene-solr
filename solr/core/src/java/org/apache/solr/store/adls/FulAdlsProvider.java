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
import java.util.List;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;

public class FulAdlsProvider implements AdlsProvider {

  ADLStoreClient adlsClient;

  public FulAdlsProvider(ADLStoreClient client){
    this.adlsClient=client;
  }

  @Override
  public boolean rename(String path1, String path2, boolean overwrite) throws IOException {
    return adlsClient.rename(path1,path2,overwrite);
  }

  @Override
  public boolean checkExists(String path) throws  IOException{
    return adlsClient.checkExists(path);
  }

  @Override
  public boolean createDirectory(String path) throws  IOException{
    return adlsClient.createDirectory(path);
  }

  @Override
  public boolean delete(String path) throws  IOException{
    return adlsClient.delete(path);
  }

  @Override
  public DirectoryEntry getDirectoryEntry(String path) throws  IOException{
    return adlsClient.getDirectoryEntry(path);
  }

  @Override
  public List<DirectoryEntry> enumerateDirectory(String path) throws  IOException{
    return adlsClient.enumerateDirectory(path);
  }

  @Override
  public ADLFileOutputStream createFile(String path, IfExists mode) throws  IOException{
    return adlsClient.createFile(path,mode);
  }

  @Override
  public ADLFileInputStream getReadStream(String path) throws  IOException{
    return adlsClient.getReadStream(path);
  }

  @Override
  public boolean deleteRecursive(String path) throws  IOException {
    return adlsClient.deleteRecursive(path);
  }

  @Override
  public ADLFileOutputStream getAppendStream(String path) throws  IOException {
    return adlsClient.getAppendStream(path);
  }




}
