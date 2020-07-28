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

package org.apache.solr.cluster.api;

import java.io.InputStream;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.solr.common.SolrException;

public interface Config {

  String name();

  /**set of files in the config */
  Set<String> resources() throws SolrException;

  /** read a file inside the config.
   * The caller should consume the stream completely and should not hold a reference to this stream.
   * This method closes the stream soon after the method returns
   * @param file  name of the file e.g: schema.xml
   */
  void  resource(Consumer<InputStream> file) throws SolrException;

}
