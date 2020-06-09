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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

/**
 * <p>
 * Gives system administrators a way to ignore very large update from clients.
 * When an update goes through processors its size can change
 * therefore this processor should be the last processor of the chain.
 * </p>
 * @since 7.4.0
 */
public class IgnoreLargeDocumentProcessorFactory extends UpdateRequestProcessorFactory {
  public static final String LIMIT_SIZE_PARAM = "limit";

  // limit of a SolrInputDocument size (in kb)
  private long maxDocumentSize = 1024 * 1024;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    maxDocumentSize = args.toSolrParams().required().getLong(LIMIT_SIZE_PARAM);
    args.remove(LIMIT_SIZE_PARAM);

    if (args.size() > 0) {
      throw new SolrException(SERVER_ERROR,
          "Unexpected init param(s): '" +
              args.getName(0) + "'");
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        long docSize = ObjectSizeEstimator.estimate(cmd.getSolrInputDocument());
        if (docSize / 1024 > maxDocumentSize) {
          throw new SolrException(BAD_REQUEST, "Size of the document "+cmd.getPrintableId()+" is too large, around:"+docSize);
        }
        super.processAdd(cmd);
      }
    };
  }

  /**
   * Util class for quickly estimate size of a {@link org.apache.solr.common.SolrInputDocument}
   * Compare to {@link org.apache.lucene.util.RamUsageEstimator}, this class have some pros
   * <ul>
   * <li>does not use reflection
   * <li>go as deep as needed to compute size of all {@link org.apache.solr.common.SolrInputField} and
   * all {@link org.apache.solr.common.SolrInputDocument} children
   * <li>compute size of String based on its length
   * <li>fast estimate size of a {@link java.util.Map} or a {@link java.util.Collection}
   * </ul>
   */
  // package private for testing
  static class ObjectSizeEstimator {
    /**
     * Sizes of primitive classes.
     */
    private static final Map<Class<?>,Integer> primitiveSizes = new IdentityHashMap<>();
    static {
      primitiveSizes.put(boolean.class, 1);
      primitiveSizes.put(Boolean.class, 1);
      primitiveSizes.put(byte.class, 1);
      primitiveSizes.put(Byte.class, 1);
      primitiveSizes.put(char.class, Character.BYTES);
      primitiveSizes.put(Character.class, Character.BYTES);
      primitiveSizes.put(short.class, Short.BYTES);
      primitiveSizes.put(Short.class, Short.BYTES);
      primitiveSizes.put(int.class, Integer.BYTES);
      primitiveSizes.put(Integer.class, Integer.BYTES);
      primitiveSizes.put(float.class, Float.BYTES);
      primitiveSizes.put(Float.class, Float.BYTES);
      primitiveSizes.put(double.class, Double.BYTES);
      primitiveSizes.put(Double.class, Double.BYTES);
      primitiveSizes.put(long.class, Long.BYTES);
      primitiveSizes.put(Long.class, Long.BYTES);
    }

    static long estimate(SolrInputDocument doc) {
      if (doc == null) return 0L;
      long size = 0;
      for (SolrInputField inputField : doc.values()) {
        size += primitiveEstimate(inputField.getName(), 0L);
        size += estimate(inputField.getValue());
      }

      if (doc.hasChildDocuments()) {
        for (SolrInputDocument childDoc : doc.getChildDocuments()) {
          size += estimate(childDoc);
        }
      }
      return size;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static long estimate(Object obj) {
      if (obj instanceof SolrInputDocument) {
        return estimate((SolrInputDocument) obj);
      }

      if (obj instanceof Map) {
        return estimate((Map) obj);
      }

      if (obj instanceof Collection) {
        return estimate((Collection) obj);
      }

      return primitiveEstimate(obj, 0L);
    }

    private static long primitiveEstimate(Object obj, long def) {
      Class<?> clazz = obj.getClass();
      if (clazz.isPrimitive()) {
        return primitiveSizes.get(clazz);
      }
      if (obj instanceof String) {
        return ((String) obj).length() * Character.BYTES;
      }
      return def;
    }

    private static long estimate(Map<Object, Object> map) {
      if (map.isEmpty()) return 0;
      long size = 0;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        size += primitiveEstimate(entry.getKey(), 0L);
        size += estimate(entry.getValue());
      }
      return size;
    }

    private static long estimate(@SuppressWarnings({"rawtypes"})Collection collection) {
      if (collection.isEmpty()) return 0;
      long size = 0;
      for (Object obj : collection) {
        size += estimate(obj);
      }
      return size;
    }
  }

}
