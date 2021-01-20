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

package org.apache.solr.client.solrj.request.beans;

import java.util.List;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

/**Just a container class for POJOs used in Package APIs
 *
 */
public class Package {
  public static class AddVersion implements ReflectMapWriter {
    @JsonProperty(value = "package", required = true)
    public String pkg;
    @JsonProperty(required = true)
    public String version;
    @JsonProperty(required = true)
    public List<String> files;
    @JsonProperty
    public String manifest;
    @JsonProperty
    public String manifestSHA512;

  }

  public static class DelVersion implements ReflectMapWriter {
    @JsonProperty(value = "package", required = true)
    public String pkg;
    @JsonProperty(required = true)
    public String version;

  }
}
