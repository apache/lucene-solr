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
package org.apache.solr.metrics.reporters;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple cache for reusable service clients used by some implementations of
 * {@link org.apache.solr.metrics.SolrMetricReporter}.
 */
public class ReporterClientCache<T> implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, T> cache = new ConcurrentHashMap<>();

  /**
   * Provide an instance of service client.
   * @param <T> formal type
   */
  public interface ClientProvider<T> {
    /**
     * Get an instance of a service client. It's not specified that each time this
     * method is invoked a new client instance should be returned.
     * @return client instance
     * @throws Exception when client creation encountered an error.
     */
    T get() throws Exception;
  }

  /**
   * Get existing or register a new client.
   * @param id client id
   * @param clientProvider provider of new client instances
   */
  public synchronized T getOrCreate(String id, ClientProvider<T> clientProvider) {
    T item = cache.get(id);
    if (item == null) {
      try {
        item = clientProvider.get();
        cache.put(id, item);
      } catch (Exception e) {
        log.warn("Error providing a new client for id={}", id, e);
        item = null;
      }
    }
    return item;
  }

  /**
   * Empty this cache, and close all clients that are {@link Closeable}.
   */
  public void close() {
    for (T client : cache.values()) {
      if (client instanceof Closeable) {
        try {
          ((Closeable)client).close();
        } catch (Exception e) {
          log.warn("Error closing client {}, ignoring...", client, e);
        }
      }
    }
    cache.clear();
  }
}
