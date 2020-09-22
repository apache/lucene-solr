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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;

/**
 * Immutable representation of binary data with version.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class VersionedData implements MapWriter {
  private final int version;
  private final byte[] data;
  private final String owner;
  private final CreateMode mode;

  /**
   * Constructor.
   * @param version version of the data, or -1 if unknown
   * @param data binary data, or null.
   * @param mode create mode
   * @param owner symbolic identifier of data owner / creator, or null.
   */
  public VersionedData(int version, byte[] data, CreateMode mode, String owner) {
    this.version = version;
    this.data = data;
    this.mode = mode;
    this.owner = owner;
  }

  public int getVersion() {
    return version;
  }

  public byte[] getData() {
    return data;
  }

  public CreateMode getMode() {
    return mode;
  }

  public String getOwner() {
    return owner;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("version", version);
    if (owner != null) {
      ew.put("owner", owner);
    }
    ew.put("mode", mode.toString());
    if (data != null) {
      ew.put("data", Base64.byteArrayToBase64(data));
    }
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VersionedData that = (VersionedData) o;
    return version == that.version &&
        Arrays.equals(data, that.data) &&
        Objects.equals(owner, that.owner) &&
        mode == that.mode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, owner);
  }
}
