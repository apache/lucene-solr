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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.ADLStoreOptions;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import com.microsoft.azure.datalake.store.IfExists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

public class MockAdlsProvider implements AdlsProvider {
  WebServerBuilder webServer;
  String authTokenEndpoint;
  String accountFQDN;
  String dummyToken;
  private ADLStoreClient adlsClient;
  private String basePath;
  private String tempDir;

  public String getAccountFQDN() {
    return accountFQDN;
  }

  public void cleanUp() {
    try {
      webServer.shutdown();
      FileUtils.deleteDirectory(new File(tempDir));
    } catch (Exception e){
      e.printStackTrace();
    }
  }

  public MockAdlsProvider() {


    String myTmpDir = System.getProperty("java.io.tmpdir");

    tempDir=new File(myTmpDir +File.separator+ UUID.randomUUID().toString()).getAbsolutePath();

    webServer = new WebServerBuilder(tempDir);
    this.basePath=tempDir;
    authTokenEndpoint = webServer.getAuthTokenEndPoint();
    accountFQDN = webServer.getAccountFQDN();
    dummyToken ="FOOFOFO";
    adlsClient = ADLStoreClient.createClient(accountFQDN, dummyToken);

    try {
      adlsClient.setOptions(new ADLStoreOptions().setInsecureTransport());
    } catch (Exception e){
      throw new RuntimeException(e);
    }

  }

  public WebServerBuilder getWebServer() {
    return webServer;
  }

  @Override
  public boolean rename(String path1, String path2, boolean overwrite) throws IOException {
    File source = new File(basePath,path1);
    File target = new File(basePath,path2);
    return source.renameTo(target);
  }

  @Override
  public boolean checkExists(String path) throws IOException {
    File source = new File(basePath,path);
    return source.exists();
  }

  @Override
  public boolean createDirectory(String path) throws IOException {
    File source = new File(basePath,path);
    return source.mkdirs();
  }

  @Override
  public boolean delete(String path) throws IOException {
    File source = new File(basePath,path);
    if (source.isDirectory()){
      throw new IllegalArgumentException("can't delete a directory!");
    }
    FileSystem fs = FileSystems.getDefault();
    Path p = fs.getPath(source.getPath());
    org.apache.lucene.util.IOUtils.deleteFilesIfExist(p);
    return true;
  }

  @Override
  public DirectoryEntry getDirectoryEntry(String path) throws IOException {
    File source = new File(basePath,path);

    DirectoryEntry entry = new DirectoryEntry(
        source.getName(),
        path,
        source.length(),
        "yomomma",
        "yomomma",
        new Date(),
        new Date(source.lastModified()),
        (source.isDirectory()? DirectoryEntryType.DIRECTORY:DirectoryEntryType.FILE),
        128,
        1,
        "777",
        false,
        new Date()
    );
    return entry;
  }

  private DirectoryEntry getDirectoryEntryQuiet(String path) {
    try {
      return getDirectoryEntry(path);
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }


  @Override
  public List<DirectoryEntry> enumerateDirectory(String path) throws IOException {
    File source = new File(basePath, path);
    return Arrays.stream(source.listFiles())
        .map((item)->getDirectoryEntryQuiet(item.getAbsolutePath()))
        .collect(Collectors.toList());
  }

  @Override
  public ADLFileOutputStream createFile(String path, IfExists mode) throws IOException {
    return adlsClient.createFile(path,mode);
  }

  @Override
  public ADLFileInputStream getReadStream(String path) throws IOException {
    return adlsClient.getReadStream(path);
  }

  @Override
  public boolean deleteRecursive(String path) throws IOException {
    FileUtils.deleteDirectory(new File(path));
    return !(new File(path).exists());
  }

  @Override
  public ADLFileOutputStream getAppendStream(String path) throws IOException {
    return adlsClient.getAppendStream(path);
  }
}
