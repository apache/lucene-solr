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
package org.apache.solr.update;

import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * An {@link InfoStream} implementation which passes messages on to Solr's logging.
 */
public class LoggingInfoStream extends InfoStream {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void message(String component, String message) {
    if (log.isInfoEnabled()) {
      log.info("[{}][{}]: {}", component, Thread.currentThread().getName(), message);
    }
  }

  @Override
  public boolean isEnabled(String component) {
    // ignore testpoints so this can be used with tests without flooding logs with verbose messages
    return !"TP".equals(component) && log.isInfoEnabled();
  }

  @Override
  public void close() throws IOException {}
}
