package org.apache.lucene.util;

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


import java.util.Locale;

/**
 * Use by certain classes to match version compatibility
 * across releases of Lucene.
 * 
 * <p><b>WARNING</b>: When changing the version parameter
 * that you supply to components in Lucene, do not simply
 * change the version at search-time, but instead also adjust
 * your indexing code to match, and re-index.
 */
// remove me when java 5 is no longer supported
// this is a workaround for a JDK bug that wrongly emits a warning.
@SuppressWarnings("dep-ann")
public enum Version {
  /**
   * Match settings and bugs in Lucene's 3.0 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_30,

  /**
   * Match settings and bugs in Lucene's 3.1 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_31,
  
  /**
   * Match settings and bugs in Lucene's 3.2 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_32,
  
  /**
   * Match settings and bugs in Lucene's 3.3 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_33,
  
  /**
   * Match settings and bugs in Lucene's 3.4 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_34,
  
  /**
   * Match settings and bugs in Lucene's 3.5 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_35,
  
  /**
   * Match settings and bugs in Lucene's 3.6 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_36,
  
  /**
   * Match settings and bugs in Lucene's 3.6 release.
   * @deprecated (4.1) Use latest
   */
  @Deprecated
  LUCENE_40,

  /** Match settings and bugs in Lucene's 4.1 release.
   * @deprecated (4.2) Use latest
   */
  @Deprecated
  LUCENE_41,

  /** Match settings and bugs in Lucene's 4.2 release.
   * @deprecated (4.3) Use latest
   */
  @Deprecated
  LUCENE_42,

  /** Match settings and bugs in Lucene's 4.3 release.
   *  <p>
   *  Use this to get the latest &amp; greatest settings, bug
   *  fixes, etc, for Lucene.
   */
  LUCENE_43,

  /* Add new constants for later versions **here** to respect order! */

  /**
   * <p><b>WARNING</b>: if you use this setting, and then
   * upgrade to a newer release of Lucene, sizable changes
   * may happen.  If backwards compatibility is important
   * then you should instead explicitly specify an actual
   * version.
   * <p>
   * If you use this constant then you  may need to 
   * <b>re-index all of your documents</b> when upgrading
   * Lucene, as the way text is indexed may have changed. 
   * Additionally, you may need to <b>re-test your entire
   * application</b> to ensure it behaves as expected, as 
   * some defaults may have changed and may break functionality 
   * in your application. 
   * @deprecated Use an actual version instead. 
   */
  @Deprecated
  LUCENE_CURRENT;

  public boolean onOrAfter(Version other) {
    return compareTo(other) >= 0;
  }
  
  public static Version parseLeniently(String version) {
    String parsedMatchVersion = version.toUpperCase(Locale.ROOT);
    return Version.valueOf(parsedMatchVersion.replaceFirst("^(\\d)\\.(\\d)$", "LUCENE_$1$2"));
  }
}
