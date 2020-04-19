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

package org.apache.solr.core;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods about paths in Solr.
 */
public final class SolrPaths {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Solr allows users to store arbitrary files in a special directory located directly under SOLR_HOME.
   * <p>
   * This directory is generally created by each node on startup.  Files located in this directory can then be
   * manipulated using select Solr features (e.g. streaming expressions).
   */
  public static final String USER_FILES_DIRECTORY = "userfiles";
  private static final Set<String> loggedOnce = new ConcurrentSkipListSet<>();

  private SolrPaths() {} // don't create this

  /**
   * Finds the solrhome based on looking up the value in one of three places:
   * <ol>
   * <li>JNDI: via java:comp/env/solr/home</li>
   * <li>The system property solr.solr.home</li>
   * <li>Look in the current working directory for a solr/ directory</li>
   * </ol>
   * <p>
   * The return value is normalized.  Normalization essentially means it ends in a trailing slash.
   *
   * @return A normalized solrhome
   * @see #normalizeDir(String)
   */
  public static Path locateSolrHome() {

    String home = null;
    // Try JNDI
    try {
      Context c = new InitialContext();
      home = (String) c.lookup("java:comp/env/solr/home");
      logOnceInfo("home_using_jndi", "Using JNDI solr.home: " + home);
    } catch (NoInitialContextException e) {
      log.debug("JNDI not configured for solr (NoInitialContextEx)");
    } catch (NamingException e) {
      log.debug("No /solr/home in JNDI");
    } catch (RuntimeException ex) {
      if (log.isWarnEnabled()) {
        log.warn("Odd RuntimeException while testing for JNDI: {}", ex.getMessage());
      }
    }

    // Now try system property
    if (home == null) {
      String prop = "solr.solr.home";
      home = System.getProperty(prop);
      if (home != null) {
        logOnceInfo("home_using_sysprop", "Using system property " + prop + ": " + home);
      }
    }

    // if all else fails, try
    if (home == null) {
      home = "solr/";
      logOnceInfo("home_default", "solr home defaulted to '" + home + "' (could not find system property or JNDI)");
    }
    return Paths.get(home);
  }

  public static void ensureUserFilesDataDir(Path solrHome) {
    final Path userFilesPath = getUserFilesPath(solrHome);
    final File userFilesDirectory = new File(userFilesPath.toString());
    if (!userFilesDirectory.exists()) {
      try {
        final boolean created = userFilesDirectory.mkdir();
        if (!created) {
          log.warn("Unable to create [{}] directory in SOLR_HOME [{}].  Features requiring this directory may fail.", USER_FILES_DIRECTORY, solrHome);
        }
      } catch (Exception e) {
        log.warn("Unable to create [{}] directory in SOLR_HOME [{}].  Features requiring this directory may fail.",
            USER_FILES_DIRECTORY, solrHome, e);
      }
    }
  }

  public static Path getUserFilesPath(Path solrHome) {
    return Paths.get(solrHome.toAbsolutePath().toString(), USER_FILES_DIRECTORY).toAbsolutePath();
  }

  /**
   * Ensures a directory name always ends with a '/'.
   */
  public static String normalizeDir(String path) {
    return (path != null && (!(path.endsWith("/") || path.endsWith("\\")))) ? path + File.separator : path;
  }

  // Logs a message only once per startup
  private static void logOnceInfo(String key, String msg) {
    if (!loggedOnce.contains(key)) {
      loggedOnce.add(key);
      log.info(msg);
    }
  }
}
