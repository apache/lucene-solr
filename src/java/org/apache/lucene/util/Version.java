package org.apache.lucene.util;

/**
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


import java.io.Serializable;

/**
 * Use by certain classes to match version compatibility
 * across releases of Lucene.
 * 
 * <p><b>WARNING</b>: When changing the version parameter
 * that you supply to components in Lucene, do not simply
 * change the version at search-time, but instead also adjust
 * your indexing code to match, and re-index.
 */
public final class Version extends Parameter implements Serializable {

  /**
   * <p><b>WARNING</b>: if you use this setting, and then
   * upgrade to a newer release of Lucene, sizable changes
   * may happen.  If backwards compatibility is important
   * then you should instead explicitly specify an actual
   * version.
   * <p>
   * If you use this constant then you may need to
   * <b>re-index all of your documents</b> when upgrading
   * Lucene, as the way text is indexed may have changed.
   * Additionally, you may need to <b>re-test your entire
   * application</b> to ensure it behaves as expected, as
   * some defaults may have changed and may break functionality
   * in your application.
   * @deprecated Use an actual version instead.
   */
  public static final Version LUCENE_CURRENT = new Version("LUCENE_CURRENT", 0);
  
  /** Match settings and bugs in Lucene's 2.0 release. */
  public static final Version LUCENE_20 = new Version("LUCENE_20", 2000);

  /** Match settings and bugs in Lucene's 2.1 release. */
  public static final Version LUCENE_21 = new Version("LUCENE_21", 2100);

  /** Match settings and bugs in Lucene's 2.2 release. */
  public static final Version LUCENE_22 = new Version("LUCENE_22", 2200);

  /** Match settings and bugs in Lucene's 2.3 release. */
  public static final Version LUCENE_23 = new Version("LUCENE_23", 2300);

  /** Match settings and bugs in Lucene's 2.4 release. */
  public static final Version LUCENE_24 = new Version("LUCENE_24", 2400);

  /** Match settings and bugs in Lucene's 2.9 release. 
   *  <p>
   *  Use this to get the latest &amp; greatest settings, bug
   *  fixes, etc, for Lucene.
   */
  public static final Version LUCENE_29 = new Version("LUCENE_29", 2900);

  private final int v;

  public Version(String name, int v) {
    super(name);
    this.v = v;
  }

  public boolean onOrAfter(Version other) {
    return v == 0 || v >= other.v;
  }
}