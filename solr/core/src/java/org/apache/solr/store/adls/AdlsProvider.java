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
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;

public interface AdlsProvider {
  boolean rename(String path1,String path2,boolean overwrite) throws IOException;
  boolean checkExists(String path) throws  IOException;
  boolean createDirectory(String path) throws  IOException;
  boolean delete(String path) throws  IOException;
  DirectoryEntry getDirectoryEntry(String path) throws  IOException;
  List<DirectoryEntry> enumerateDirectory(String path) throws  IOException;
  ADLFileOutputStream createFile(String path, IfExists mode) throws  IOException;
  ADLFileInputStream getReadStream(String path) throws  IOException;
  boolean deleteRecursive(String path) throws  IOException;
  ADLFileOutputStream getAppendStream(String path) throws  IOException;
}
