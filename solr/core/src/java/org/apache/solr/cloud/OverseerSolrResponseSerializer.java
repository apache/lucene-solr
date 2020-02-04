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
package org.apache.solr.cloud;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;

public class OverseerSolrResponseSerializer {
  
  /**
   * This method serializes the content of an {@code OverseerSolrResponse}. Note that:
   * <ul>
   * <li>The elapsed time is not serialized</li>
   * <li>"Unknown" elements for the Javabin format will be serialized as Strings. See {@link org.apache.solr.common.util.JavaBinCodec#writeVal}</li>
   * </ul>
   */
  @SuppressWarnings("deprecation")
  public static byte[] serialize(OverseerSolrResponse responseObject) {
    Objects.requireNonNull(responseObject);
    if (useUnsafeSerialization()) {
      return SolrResponse.serializable(responseObject);
    }
    try {
      return IOUtils.toByteArray(Utils.toJavabin(responseObject.getResponse()));
    } catch (IOException|RuntimeException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Exception serializing response to Javabin", e);
    }
  }
  
  static boolean useUnsafeSerialization() {
    String useUnsafeOverseerResponse = System.getProperty("solr.useUnsafeOverseerResponse");
    return useUnsafeOverseerResponse != null && ("true".equals(useUnsafeOverseerResponse));
  }
  
  static boolean useUnsafeDeserialization() {
    String useUnsafeOverseerResponse = System.getProperty("solr.useUnsafeOverseerResponse");
    return useUnsafeOverseerResponse != null && ("true".equals(useUnsafeOverseerResponse) || "deserialization".equals(useUnsafeOverseerResponse));
  }

  @SuppressWarnings("deprecation")
  public static OverseerSolrResponse deserialize(byte[] responseBytes) {
    Objects.requireNonNull(responseBytes);
    try {
      @SuppressWarnings("unchecked")
      NamedList<Object> response = (NamedList<Object>) Utils.fromJavabin(responseBytes);
      return new OverseerSolrResponse(response);
    } catch (IOException|RuntimeException e) {
      if (useUnsafeDeserialization()) {
        return (OverseerSolrResponse) SolrResponse.deserialize(responseBytes);
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Exception deserializing response from Javabin", e);
    }
  }

}
