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
public enum Version {
  /**
   * Match settings and bugs in Lucene's 3.0 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_3_0,

  /**
   * Match settings and bugs in Lucene's 3.1 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_3_1,
  
  /**
   * Match settings and bugs in Lucene's 3.2 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_3_2,
  
  /**
   * Match settings and bugs in Lucene's 3.3 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_3_3,
  
  /**
   * Match settings and bugs in Lucene's 3.4 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_3_4,
  
  /**
   * Match settings and bugs in Lucene's 3.5 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_3_5,
  
  /**
   * Match settings and bugs in Lucene's 3.6 release.
   * @deprecated (4.0) Use latest
   */
  @Deprecated
  LUCENE_3_6,
  
  /**
   * Match settings and bugs in Lucene's 4.0 release.
   * @deprecated (4.1) Use latest
   */
  @Deprecated
  LUCENE_4_0,

  /** Match settings and bugs in Lucene's 4.1 release.
   * @deprecated (4.2) Use latest
   */
  @Deprecated
  LUCENE_4_1,

  /** Match settings and bugs in Lucene's 4.2 release.
   * @deprecated (4.3) Use latest
   */
  @Deprecated
  LUCENE_4_2,

  /** Match settings and bugs in Lucene's 4.3 release.
   * @deprecated (4.4) Use latest
   */
  @Deprecated
  LUCENE_4_3,

  /** Match settings and bugs in Lucene's 4.4 release.
   * @deprecated (4.5) Use latest
   */
  @Deprecated
  LUCENE_4_4,

  /**
   * Match settings and bugs in Lucene's 4.5 release.
   * @deprecated (4.6) Use latest
   */
  @Deprecated
  LUCENE_4_5,

  /**
   * Match settings and bugs in Lucene's 4.6 release.
   * @deprecated (4.7) Use latest
   */
  @Deprecated
  LUCENE_4_6,

  /** Match settings and bugs in Lucene's 4.7 release.
   * @deprecated (4.8) Use latest
   */
  @Deprecated
  LUCENE_4_7,

  /**
   * Match settings and bugs in Lucene's 4.8 release.
   * @deprecated (4.9) Use latest
   */
  @Deprecated
  LUCENE_4_8,

  /** Match settings and bugs in Lucene's 4.9 release.
   *  <p>
   *  Use this to get the latest &amp; greatest settings, bug
   *  fixes, etc, for Lucene.
   */
  LUCENE_4_9,

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

  
  // Deprecated old version constants, just for backwards compatibility:
  // Those are no longer enum constants and don't work in switch statements,
  // but should fix most uses.
  // TODO: Do not update them anymore, deprecated in 4.9, so LUCENE_49 is not needed!
  // Remove in 5.0!
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_3_0} instead. */
  @Deprecated
  public static final Version LUCENE_30 = LUCENE_3_0;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_3_1} instead. */
  @Deprecated
  public static final Version LUCENE_31 = LUCENE_3_1;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_3_2} instead. */
  @Deprecated
  public static final Version LUCENE_32 = LUCENE_3_2;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_3_3} instead. */
  @Deprecated
  public static final Version LUCENE_33 = LUCENE_3_3;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_3_4} instead. */
  @Deprecated
  public static final Version LUCENE_34 = LUCENE_3_4;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_3_5} instead. */
  @Deprecated
  public static final Version LUCENE_35 = LUCENE_3_5;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_3_6} instead. */
  @Deprecated
  public static final Version LUCENE_36 = LUCENE_3_6;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_0} instead. */
  @Deprecated
  public static final Version LUCENE_40 = LUCENE_4_0;
  
  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_1} instead. */
  @Deprecated
  public static final Version LUCENE_41 = LUCENE_4_1;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_2} instead. */
  @Deprecated
  public static final Version LUCENE_42 = LUCENE_4_2;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_3} instead. */
  @Deprecated
  public static final Version LUCENE_43 = LUCENE_4_3;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_4} instead. */
  @Deprecated
  public static final Version LUCENE_44 = LUCENE_4_4;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_5} instead. */
  @Deprecated
  public static final Version LUCENE_45 = LUCENE_4_5;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_6} instead. */
  @Deprecated
  public static final Version LUCENE_46 = LUCENE_4_6;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_7} instead. */
  @Deprecated
  public static final Version LUCENE_47 = LUCENE_4_7;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_8} instead. */
  @Deprecated
  public static final Version LUCENE_48 = LUCENE_4_8;
  
  // End: Deprecated version constants -> Remove in 5.0

  public boolean onOrAfter(Version other) {
    return compareTo(other) >= 0;
  }
  
  public static Version parseLeniently(String version) {
    final String parsedMatchVersion = version
        .toUpperCase(Locale.ROOT)
        .replaceFirst("^(\\d+)\\.(\\d+)$", "LUCENE_$1_$2")
        .replaceFirst("^LUCENE_(\\d)(\\d)$", "LUCENE_$1_$2");
    return Version.valueOf(parsedMatchVersion);
  }
}
