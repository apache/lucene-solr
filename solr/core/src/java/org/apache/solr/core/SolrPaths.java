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

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import org.apache.commons.exec.OS;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods about paths in Solr.
 */
public final class SolrPaths {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrPaths() {} // don't create this

  /**
   * Ensures a directory name always ends with a '/'.
   */
  public static String normalizeDir(String path) {
    return (path != null && (!(path.endsWith("/") || path.endsWith("\\")))) ? path + File.separator : path;
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
    final Path path = Path.of(pathToAssert.toString()).normalize();
    if (path.startsWith("..")) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Path " + pathToAssert + " disallowed due to path traversal..");
    }
    if (!path.isAbsolute()) return; // All relative paths are accepted
    if (allowPaths.contains(Paths.get("_ALL_"))) return; // Catch-all path "*"/"_ALL_" will allow all other paths
    if (allowPaths.stream().noneMatch(p -> path.startsWith(Path.of(p.toString())))) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Path " + path + " must be relative to SOLR_HOME, SOLR_DATA_HOME coreRootDirectory. Set system property 'solr.allowPaths' to add other allowed paths.");
    }
  }
}
