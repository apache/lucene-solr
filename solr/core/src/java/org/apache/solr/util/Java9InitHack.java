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

import java.lang.invoke.MethodHandles;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

import org.apache.lucene.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class works around a bug in hadoop-common-2.7.2 where the Hadoop Shell class cannot
 * initialize on Java 9 (due to a bug while parsing Java's version number).
 * This class does some early checks and fakes the java version for a very short time
 * during class loading of Solr's web application or Solr's test framework.
 * <p>
 * Be sure to run this only in static initializers, as soon as possible after JVM startup!
 * <p>
 * Related issues: HADOOP-14586, SOLR-10966
 * <p>
 * TODO: <b>Remove this ASAP, once we have upgraded Hadoop (SOLR-10951)!</b>
 * 
 * @lucene.internal
 */
public final class Java9InitHack {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final String JAVA_VERSION_PROP = "java.version";
  private static boolean done = false;

  /**
   * Runs the hack. Should be done as early as possible on JVM startup, from a static initializer
   * to prevent concurrency issues - because we change temporarily some 'important' system properties.
   */
  public static synchronized void initJava9() {
    if (Constants.JRE_IS_MINIMUM_JAVA9 && done == false) {
      AccessController.doPrivileged((PrivilegedAction<Void>) Java9InitHack::initPrivileged);
      done = true;
    }
  }
     
  private static Void initPrivileged() {
    log.info("Adding temporary workaround for Hadoop's Shell class to allow running on Java 9 (please ignore any warnings/failures).");
    String oldVersion = System.getProperty(JAVA_VERSION_PROP);
    try {
      System.setProperty(JAVA_VERSION_PROP, "1.9");
      Class.forName("org.apache.hadoop.util.Shell");
    } catch (Throwable t) {
      log.warn("Cannot initialize Hadoop's Shell class on Java 9.", t);
    } finally {
      if (!Objects.equals(System.getProperty(JAVA_VERSION_PROP), oldVersion)) {
        System.setProperty(JAVA_VERSION_PROP, oldVersion);
      }
    }
    return null;
  }
  
  private Java9InitHack() {}
    
}
