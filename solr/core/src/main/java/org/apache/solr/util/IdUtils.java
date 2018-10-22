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

import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.util.TimeSource;

/**
 * Helper class for generating unique ID-s.
 */
public class IdUtils {

  /**
   * Generate a short random id (see {@link StringHelper#randomId()}).
   */
  public static final String randomId() {
    return StringHelper.idToString(StringHelper.randomId());
  }

  /**
   * Generate a random id with a timestamp, in the format:
   * <code>hex(timestamp) + 'T' + randomId</code>. This method
   * uses {@link TimeSource#CURRENT_TIME} for timestamp values.
   */
  public static final String timeRandomId() {
    return timeRandomId(TimeUnit.MILLISECONDS.convert(TimeSource.CURRENT_TIME.getTimeNs(), TimeUnit.NANOSECONDS));
  }

  /**
   * Generate a random id with a timestamp, in the format:
   * <code>hex(timestamp) + 'T' + randomId</code>.
   * @param time value representing timestamp
   */
  public static final String timeRandomId(long time) {
    StringBuilder sb = new StringBuilder(Long.toHexString(time));
    sb.append('T');
    sb.append(randomId());
    return sb.toString();
  }
}
