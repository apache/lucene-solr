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

package org.apache.solr.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.xml.sax.InputSource;

public class InputSourceUtil {
  public static InputSource populate(SolrZkClient client, String path, Stat stat) {
    try {
      byte[] data = client.getData(path, null, stat, true);
      InputStream schemaInputStream = new ByteArrayInputStream(data);
      return new InputSource(schemaInputStream);
    } catch (KeeperException.NoNodeException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Configset path:" + path + " not found", e);
    } catch (InterruptedException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to access path:" + path, e);
    }
  }
}
