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
public final class Version {

  /**
   * Match settings and bugs in Lucene's 4.0.0-ALPHA release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_0_0_ALPHA = new Version(4, 0, 0, 0);

  /**
   * Match settings and bugs in Lucene's 4.0.0-BETA release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_0_0_BETA = new Version(4, 0, 0, 1);

  /**
   * Match settings and bugs in Lucene's 4.0.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_0_0 = new Version(4, 0, 0, 2);
  
  /**
   * Match settings and bugs in Lucene's 4.1.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_1_0 = new Version(4, 1, 0);

  /**
   * Match settings and bugs in Lucene's 4.2.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_2_0 = new Version(4, 2, 0);

  /**
   * Match settings and bugs in Lucene's 4.2.1 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_2_1 = new Version(4, 2, 1);

  /**
   * Match settings and bugs in Lucene's 4.3.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_3_0 = new Version(4, 3, 0);

  /**
   * Match settings and bugs in Lucene's 4.3.1 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_3_1 = new Version(4, 3, 1);

  /**
   * Match settings and bugs in Lucene's 4.4.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_4_0 = new Version(4, 4, 0);

  /**
   * Match settings and bugs in Lucene's 4.5.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_5_0 = new Version(4, 5, 0);

  /**
   * Match settings and bugs in Lucene's 4.5.1 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_5_1 = new Version(4, 5, 1);

  /**
   * Match settings and bugs in Lucene's 4.6.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_6_0 = new Version(4, 6, 0);

  /**
   * Match settings and bugs in Lucene's 4.6.1 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_6_1 = new Version(4, 6, 1);
  
  /**
   * Match settings and bugs in Lucene's 4.7.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_7_0 = new Version(4, 7, 0);

  /**
   * Match settings and bugs in Lucene's 4.7.1 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_7_1 = new Version(4, 7, 1);

  /**
   * Match settings and bugs in Lucene's 4.7.2 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_7_2 = new Version(4, 7, 2);
  
  /**
   * Match settings and bugs in Lucene's 4.8.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_8_0 = new Version(4, 8, 0);

  /**
   * Match settings and bugs in Lucene's 4.8.1 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_8_1 = new Version(4, 8, 1);

  /**
   * Match settings and bugs in Lucene's 4.9.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_9_0 = new Version(4, 9, 0);
  
  /**
   * Match settings and bugs in Lucene's 4.10 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_10_0 = new Version(4, 10, 0);

  /**
   * Match settings and bugs in Lucene's 4.11.0 release.
   * @deprecated (5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_4_11_0 = new Version(4, 11, 0);

  /** Match settings and bugs in Lucene's 5.0 release.
   *  <p>
   *  Use this to get the latest &amp; greatest settings, bug
   *  fixes, etc, for Lucene.
   */
  public static final Version LUCENE_5_0_0 = new Version(5, 0, 0);

  // To add a new version:
  //  * Only add above this comment
  //  * If the new version is the newest, change LATEST below and deprecate the previous LATEST

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
   */
  public static final Version LATEST = LUCENE_5_0_0;

  /**
   * Constant for backwards compatibility.
   * @deprecated Use {@link #LATEST}
   */
  @Deprecated
  public static final Version LUCENE_CURRENT = LATEST;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_0_0} instead (this constant actually points to {@link #LUCENE_4_0_0_ALPHA} to match whole 4.0 series). */
  @Deprecated
  public static final Version LUCENE_4_0 = LUCENE_4_0_0_ALPHA;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_1_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_1 = LUCENE_4_1_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_2_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_2 = LUCENE_4_2_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_3_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_3 = LUCENE_4_3_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_4_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_4 = LUCENE_4_4_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_5_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_5 = LUCENE_4_5_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_6_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_6 = LUCENE_4_6_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_7_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_7 = LUCENE_4_7_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_8_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_8 = LUCENE_4_8_0;

  /** @deprecated Bad naming of constant; use {@link #LUCENE_4_9_0} instead. */
  @Deprecated
  public static final Version LUCENE_4_9 = LUCENE_4_9_0;


  /**
   * Parse a version number of the form {@code "major.minor.bugfix.prerelease"}.
   *
   * Part {@code ".bugfix"} and part {@code ".prerelease"} are optional.
   * Note that this is forwards compatible: the parsed version does not have to exist as
   * a constant.
   */
  public static Version parse(String version) {
    String[] pieces = version.split("\\.");
    if (pieces.length < 2 || pieces.length > 4) {
      throw new IllegalArgumentException("Version is not in form major.minor.bugfix(.prerelease): " + version);
    }

    int major = Integer.parseInt(pieces[0]);
    int minor = Integer.parseInt(pieces[1]);
    int bugfix = 0;
    int prerelease = 0;
    if (pieces.length > 2) {
      bugfix = Integer.parseInt(pieces[2]);
    }
    if (pieces.length > 3) {
      prerelease = Integer.parseInt(pieces[3]);
      if (prerelease == 0) {
        throw new IllegalArgumentException("Invalid value " + prerelease + " for prerelease of version " + version +", should be 1 or 2");
      }
    }

    return new Version(major, minor, bugfix, prerelease);
  }

  /**
   * Parse the given version number as a constant or dot based version.
   * <p>This method allows to use {@code "LUCENE_X_Y"} constant names,
   * or version numbers in the format {@code "x.y.z"}.
   */
  public static Version parseLeniently(String version) {
    version = version.toUpperCase(Locale.ROOT);
    switch (version) {
      case "LATEST":
      case "LUCENE_CURRENT":
        return LATEST;
      case "LUCENE_4_0_0":
        return LUCENE_4_0_0;
      case "LUCENE_4_0_0_ALPHA":
        return LUCENE_4_0_0_ALPHA;
      case "LUCENE_4_0_0_BETA":
        return LUCENE_4_0_0_BETA;
      default:
        version = version
          .replaceFirst("^LUCENE_(\\d+)_(\\d+)_(\\d+)$", "$1.$2.$3")
          .replaceFirst("^LUCENE_(\\d+)_(\\d+)$", "$1.$2.0")
          .replaceFirst("^LUCENE_(\\d)(\\d)$", "$1.$2.0");
        return parse(version);
    }
  }

  /** Major version, the difference between stable and trunk */
  public final int major;
  /** Minor version, incremented within the stable branch */
  public final int minor;
  /** Bugfix number, incremented on release branches */
  public final int bugfix;
  /** Prerelease version, currently 0 (alpha), 1 (beta), or 2 (final) */
  public final int prerelease;

  // stores the version pieces, with most significant pieces in high bits
  // ie:  | 1 byte | 1 byte | 1 byte |   2 bits   |
  //         major   minor    bugfix   prerelease
  private final int encodedValue;

  private Version(int major, int minor, int bugfix) {
    this(major, minor, bugfix, 0);
  }

  private Version(int major, int minor, int bugfix, int prerelease) {
    this.major = major;
    this.minor = minor;
    this.bugfix = bugfix;
    this.prerelease = prerelease;
    if (major > 5 || major < 4) {
      throw new IllegalArgumentException("Lucene 5.x only supports 5.x and 4.x versions");
    }
    if (minor > 255 | minor < 0) {
      throw new IllegalArgumentException("Illegal minor version: " + minor);
    }
    if (bugfix > 255 | bugfix < 0) {
      throw new IllegalArgumentException("Illegal bugfix version: " + bugfix);
    }
    if (prerelease > 2 | prerelease < 0) {
      throw new IllegalArgumentException("Illegal prerelease version: " + prerelease);
    }
    if (prerelease != 0 && (minor != 0 || bugfix != 0)) {
      throw new IllegalArgumentException("Prerelease version only supported with major release");
    }

    encodedValue = major << 18 | minor << 10 | bugfix << 2 | prerelease;
  }

  /**
   * Returns true if this version is the same or after the version from the argument.
   */
  public boolean onOrAfter(Version other) {
    return encodedValue >= other.encodedValue;
  }

  @Override
  public String toString() {
    int major = (encodedValue >>> 18) & 0xFF;
    int minor = (encodedValue >>> 10) & 0xFF;
    int bugfix = (encodedValue >>> 2) & 0xFF;
    int prerelease = encodedValue & 0x3;
    if (prerelease == 0) {
      return "" + major + "." + minor + "." + bugfix;
    }
    return "" + major + "." + minor + "." + bugfix + "." + prerelease;
  }

  @Override
  public boolean equals(Object o) {
    return o != null && o instanceof Version && ((Version)o).encodedValue == encodedValue;
  }

  @Override
  public int hashCode() {
    return encodedValue;
  }
}
