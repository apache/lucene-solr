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
package org.apache.solr.client.solrj.cloud.autoscaling;

/**
 * Immutable representation of binary data with version.
 */
public class VersionedData {
  private final int version;
  private final byte[] data;
  private final String owner;

  /**
   * Constructor.
   * @param version version of the data, or -1 if unknown
   * @param data binary data, or null.
   * @param owner symbolic identifier of data owner / creator, or null.
   */
  public VersionedData(int version, byte[] data, String owner) {
    this.version = version;
    this.data = data;
    this.owner = owner;
  }

  public int getVersion() {
    return version;
  }

  public byte[] getData() {
    return data;
  }

  public String getOwner() {
    return owner;
  }
}
