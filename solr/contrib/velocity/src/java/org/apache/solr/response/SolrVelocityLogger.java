package org.apache.solr.response;

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

import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.log.LogChute;
import org.slf4j.Logger;

public class SolrVelocityLogger implements LogChute {
  private final Logger log;

  public SolrVelocityLogger(Logger log) {
    this.log = log;
  }

  @Override
  public void init(RuntimeServices runtimeServices) throws Exception {
  }

  @Override
  public void log(int level, String message) {
    switch(level) {
      case LogChute.TRACE_ID:
        log.trace(message);
        break;

      case LogChute.DEBUG_ID:
        log.debug(message);
        break;

      case LogChute.INFO_ID:
        log.info(message);
        break;

      case LogChute.WARN_ID:
        log.warn(message);
        break;

      case LogChute.ERROR_ID:
        log.error(message);
        break;

      default: // unknown logging level, use warn
        log.warn(message);
        break;
    }
  }

  @Override
  public void log(int level, String message, Throwable throwable) {
    switch(level) {
      case LogChute.TRACE_ID:
        log.trace(message, throwable);
        break;

      case LogChute.DEBUG_ID:
        log.debug(message, throwable);
        break;

      case LogChute.INFO_ID:
        log.info(message, throwable);
        break;

      case LogChute.WARN_ID:
        log.warn(message, throwable);
        break;

      case LogChute.ERROR_ID:
        log.error(message, throwable);
        break;

      default: // unknown logging level, use warn
        log.warn(message, throwable);
        break;
    }
  }

  @Override
  public boolean isLevelEnabled(int level) {
    switch(level) {
      case LogChute.TRACE_ID:
        return log.isTraceEnabled();

      case LogChute.DEBUG_ID:
        return log.isDebugEnabled();

      case LogChute.INFO_ID:
        return log.isInfoEnabled();

      case LogChute.WARN_ID:
        return log.isWarnEnabled();

      case LogChute.ERROR_ID:
        return log.isErrorEnabled();

      default:
        return false;
    }
  }
}
