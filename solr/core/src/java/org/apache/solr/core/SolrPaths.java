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

import org.apache.commons.exec.OS;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods about paths in Solr.
 */
public final class SolrPaths {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
   * @deprecated all code should get solr home from CoreContainer
   * @see CoreContainer#getSolrHome()
   */
  @Deprecated
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
      log.warn("Odd RuntimeException while testing for JNDI: ", ex);
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

  /**
   * Checks that the given path is relative to one of the allowPaths supplied. Typically this will be
   * called from {@link CoreContainer#assertPathAllowed(Path)} and allowPaths pre-filled with the node's
   * SOLR_HOME, SOLR_DATA_HOME and coreRootDirectory folders, as well as any paths specified in
   * solr.xml's allowPaths element. The following paths will always fail validation:
   * <ul>
   *   <li>Relative paths starting with <code>..</code></li>
   *   <li>Windows UNC paths (such as <code>\\host\share\path</code>)</li>
   *   <li>Paths which are not relative to any of allowPaths</li>
   * </ul>
   * @param pathToAssert path to check
   * @param allowPaths list of paths that should be allowed prefixes for pathToAssert
   * @throws SolrException if path is outside allowed paths
   */
  public static void assertPathAllowed(Path pathToAssert, Set<Path> allowPaths) throws SolrException {
    if (pathToAssert == null) return;
    if (OS.isFamilyWindows() && pathToAssert.toString().startsWith("\\\\")) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Path " + pathToAssert + " disallowed. UNC paths not supported. Please use drive letter instead.");
    }
    // Conversion Path -> String -> Path is to be able to compare against org.apache.lucene.mockfile.FilterPath instances
    final Path path = Paths.get(pathToAssert.toString()).normalize();
    if (path.startsWith("..")) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Path " + pathToAssert + " disallowed due to path traversal..");
    }
    if (!path.isAbsolute()) return; // All relative paths are accepted
    if (allowPaths.contains(Paths.get("_ALL_"))) return; // Catch-all path "*"/"_ALL_" will allow all other paths
    if (allowPaths.stream().noneMatch(p -> path.startsWith(Paths.get(p.toString())))) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Path " + path + " must be relative to SOLR_HOME, SOLR_DATA_HOME coreRootDirectory. Set system property 'solr.allowPaths' to add other allowed paths.");
    }
  }
}
