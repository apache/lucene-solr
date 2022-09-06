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

package org.apache.solr.common.util;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows random faults to be injected in running code during test runs across all solr packages.
 *
 * @lucene.internal
 */
public class CommonTestInjection {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile static Map<String, String> additionalSystemProps = null;

  public static void reset() {
    additionalSystemProps = null;
    delay = null;
  }

  private static volatile Integer delay = null;

  public static void setAdditionalProps(Map<String, String> additionalSystemProps) {
    CommonTestInjection.additionalSystemProps = additionalSystemProps;
  }

  public static Map<String,String> injectAdditionalProps() {
    return additionalSystemProps;
  }

  /**
   * Set test delay (sleep) in unit of millisec
   *
   * @param delay delay in millisec, null to remove such delay
   */
  public static void setDelay(Integer delay) {
    CommonTestInjection.delay = delay;
  }

  /**
   * Inject an artificial delay(sleep) into the code
   *
   * @return true
   */
  public static boolean injectDelay() {
    if (delay != null) {
      try {
        log.info("Start: artificial delay for {}ms", delay);
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        log.info("Finish: artificial delay for {}ms", delay);
      }
    }
    return true;
  }
}
