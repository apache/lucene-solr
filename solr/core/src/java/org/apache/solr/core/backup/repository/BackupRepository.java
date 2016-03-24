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
package org.apache.solr.core.backup.repository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public interface BackupRepository {
  /**
   * This method creates a URI using the specified base location (during object creation) and the path
   * components (as method arguments).
   * @param pathComponents The directory (or file-name) to be included in the URI.
   * @return A URI containing absolute path
   */
   URI createURI(String... pathComponents);

   /**
    * This method returns a {@linkplain OutputStream} instance for the specified <code>path</code>
    *
    * @param path The path for which {@linkplain OutputStream} needs to be created
    * @return {@linkplain OutputStream} instance for the specified <code>path</code>
    * @throws IOException in case of errors
    */
   OutputStream createOutput(URI path) throws IOException;

  /**
   * This method checks if the specified path exists in this repository.
   *
   * @param path The path whose existence needs to be checked.
   * @return if the specified path exists in this repository.
   * @throws IOException in case of errors
   */
  boolean exists(URI path) throws IOException;

   /**
    * This method creates a directory at the specified path.
    *
    * @param path The path where the directory needs to be created.
    * @throws IOException in case of errors
    */
   void createDirectory(URI path) throws IOException;

   /**
    * This method deletes a directory at the specified path.
    *
    * @param path The path referring to the directory to be deleted.
    * @throws IOException  in case of errors
    */
   void deleteDirectory(URI path) throws IOException;
}
