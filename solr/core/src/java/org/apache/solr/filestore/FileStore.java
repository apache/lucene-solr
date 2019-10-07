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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.apache.solr.common.MapWriter;
import org.apache.solr.filestore.FileStoreAPI.MetaData;


public interface FileStore {

  /**Store a file into the filestore. This should ensure that it is replicated
   * across all nodes in the cluster
   */
  void put(String path , MetaData metadata, ByteBuffer filecontent) throws IOException;

  /** read file content from a given path
   */
  void get(String path, BiConsumer<InputStream, MetaData> filecontent) throws IOException;

  /**Fetch a resource from another node
   * internal
   */
  void fetch(String path, String from);

  List<MapWriter> list(String path, Predicate<String> predicate);

  /**get the real path on filesystem
   */
  Path getRealpath(String path);

  /**The type of the resource
   */
  FileType getType(String path);

  enum FileType {
    FILE, DIRECTORY, NOFILE, METADATA
  }


}
