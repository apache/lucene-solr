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
package org.apache.lucene.util;



import java.text.ParseException;
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
   * Match settings and bugs in Lucene's 7.0.0 release.
   * @deprecated (8.0.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_0_0 = new Version(7, 0, 0);

  /**
   * Match settings and bugs in Lucene's 7.0.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_0_1 = new Version(7, 0, 1);

  /**
   * Match settings and bugs in Lucene's 7.1.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_1_0 = new Version(7, 1, 0);

  /**
   * Match settings and bugs in Lucene's 7.2.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_2_0 = new Version(7, 2, 0);

  /**
   * Match settings and bugs in Lucene's 7.2.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_2_1 = new Version(7, 2, 1);


  /**
   * Match settings and bugs in Lucene's 7.3.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_3_0 = new Version(7, 3, 0);

  /**
   * Match settings and bugs in Lucene's 7.3.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_3_1 = new Version(7, 3, 1);

  /**
   * Match settings and bugs in Lucene's 7.4.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_4_0 = new Version(7, 4, 0);

  /**
   * Match settings and bugs in Lucene's 7.5.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_5_0 = new Version(7, 5, 0);

  /**
   * Match settings and bugs in Lucene's 7.6.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_6_0 = new Version(7, 6, 0);

  /**
   * Match settings and bugs in Lucene's 7.7.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_7_0 = new Version(7, 7, 0);

  /**
   * Match settings and bugs in Lucene's 7.7.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_7_1 = new Version(7, 7, 1);

  /**
   * Match settings and bugs in Lucene's 7.7.2 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_7_2 = new Version(7, 7, 2);

  /**
   * Match settings and bugs in Lucene's 7.7.2 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_7_3 = new Version(7, 7, 3);

  /**
   * Match settings and bugs in Lucene's 7.8.0 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_7_8_0 = new Version(7, 8, 0);

  /**
   * Match settings and bugs in Lucene's 8.0.0 release.
   * @deprecated (8.1.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_0_0 = new Version(8, 0, 0);

  /**
   * Match settings and bugs in Lucene's 8.1.0 release.
   * @deprecated (8.2.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_1_0 = new Version(8, 1, 0);

  /**
   * Match settings and bugs in Lucene's 8.1.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_1_1 = new Version(8, 1, 1);

  /**
   * Match settings and bugs in Lucene's 8.2.0 release.
   * @deprecated (8.3.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_2_0 = new Version(8, 2, 0);

  /**
   * Match settings and bugs in Lucene's 8.3.0 release.
   * @deprecated (8.4.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_3_0 = new Version(8, 3, 0);

  /**
   * Match settings and bugs in Lucene's 8.3.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_3_1 = new Version(8, 3, 1);

  /**
   * Match settings and bugs in Lucene's 8.4.0 release.
   * @deprecated (8.5.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_4_0 = new Version(8, 4, 0);

  /**
   * Match settings and bugs in Lucene's 8.4.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_4_1 = new Version(8, 4, 1);

  /**
   * Match settings and bugs in Lucene's 8.5.0 release.
   * @deprecated (8.6.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_5_0 = new Version(8, 5, 0);

  /**
   * Match settings and bugs in Lucene's 8.5.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_5_1 = new Version(8, 5, 1);

  /**
   * Match settings and bugs in Lucene's 8.5.2 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_5_2 = new Version(8, 5, 2);

  /**
   * Match settings and bugs in Lucene's 8.6.0 release.
   * @deprecated (8.7.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_6_0 = new Version(8, 6, 0);

  /**
   * Match settings and bugs in Lucene's 8.6.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_6_1 = new Version(8, 6, 1);

  /**
   * Match settings and bugs in Lucene's 8.6.2 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_6_2 = new Version(8, 6, 2);

  /**
   * Match settings and bugs in Lucene's 8.6.3 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_6_3 = new Version(8, 6, 3);

  /**
   * Match settings and bugs in Lucene's 8.7.0 release.
   * @deprecated (8.8.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_7_0 = new Version(8, 7, 0);

  /**
   * Match settings and bugs in Lucene's 8.8.0 release.
   * @deprecated (8.9.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_8_0 = new Version(8, 8, 0);

  /**
   * Match settings and bugs in Lucene's 8.8.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_8_1 = new Version(8, 8, 1);

  /**
   * Match settings and bugs in Lucene's 8.8.2 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_8_2 = new Version(8, 8, 2);

  /**
   * Match settings and bugs in Lucene's 8.9.0 release.
   * @deprecated (8.10.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_9_0 = new Version(8, 9, 0);

  /**
   * Match settings and bugs in Lucene's 8.10.0 release.
   * @deprecated (8.11.0) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_10_0 = new Version(8, 10, 0);

  /**
   * Match settings and bugs in Lucene's 8.10.1 release.
   * @deprecated Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_10_1 = new Version(8, 10, 1);

  /**
   * Match settings and bugs in Lucene's 8.11.0 release.
   * @deprecated (8.11.1) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_11_0 = new Version(8, 11, 0);

  /**
   * Match settings and bugs in Lucene's 8.11.1 release.
   * @deprecated (8.11.2) Use latest
   */
  @Deprecated
  public static final Version LUCENE_8_11_1 = new Version(8, 11, 1);

  /**
   * Match settings and bugs in Lucene's 8.11.2 release.
   * <p>
   * Use this to get the latest &amp; greatest settings, bug
   * fixes, etc, for Lucene.
   */
  public static final Version LUCENE_8_11_2 = new Version(8, 11, 2);

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
  public static final Version LATEST = LUCENE_8_11_2;

  /**
   * Constant for backwards compatibility.
   * @deprecated Use {@link #LATEST}
   */
  @Deprecated
  public static final Version LUCENE_CURRENT = LATEST;

  /**
   * Parse a version number of the form {@code "major.minor.bugfix.prerelease"}.
   *
   * Part {@code ".bugfix"} and part {@code ".prerelease"} are optional.
   * Note that this is forwards compatible: the parsed version does not have to exist as
   * a constant.
   *
   * @lucene.internal
   */
  public static Version parse(String version) throws ParseException {

    StrictStringTokenizer tokens = new StrictStringTokenizer(version, '.');
    if (tokens.hasMoreTokens() == false) {
      throw new ParseException("Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
    }

    int major;
    String token = tokens.nextToken();
    try {
      major = Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      ParseException p = new ParseException("Failed to parse major version from \"" + token + "\" (got: " + version + ")", 0);
      p.initCause(nfe);
      throw p;
    }

    if (tokens.hasMoreTokens() == false) {
      throw new ParseException("Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
    }

    int minor;
    token = tokens.nextToken();
    try {
      minor = Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      ParseException p = new ParseException("Failed to parse minor version from \"" + token + "\" (got: " + version + ")", 0);
      p.initCause(nfe);
      throw p;
    }

    int bugfix = 0;
    int prerelease = 0;
    if (tokens.hasMoreTokens()) {

      token = tokens.nextToken();
      try {
        bugfix = Integer.parseInt(token);
      } catch (NumberFormatException nfe) {
        ParseException p = new ParseException("Failed to parse bugfix version from \"" + token + "\" (got: " + version + ")", 0);
        p.initCause(nfe);
        throw p;
      }

      if (tokens.hasMoreTokens()) {
        token = tokens.nextToken();
        try {
          prerelease = Integer.parseInt(token);
        } catch (NumberFormatException nfe) {
          ParseException p = new ParseException("Failed to parse prerelease version from \"" + token + "\" (got: " + version + ")", 0);
          p.initCause(nfe);
          throw p;
        }
        if (prerelease == 0) {
          throw new ParseException("Invalid value " + prerelease + " for prerelease; should be 1 or 2 (got: " + version + ")", 0);
        }

        if (tokens.hasMoreTokens()) {
          // Too many tokens!
          throw new ParseException("Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
        }
      }
    }

    try {
      return new Version(major, minor, bugfix, prerelease);
    } catch (IllegalArgumentException iae) {
      ParseException pe = new ParseException("failed to parse version string \"" + version + "\": " + iae.getMessage(), 0);
      pe.initCause(iae);
      throw pe;
    }
  }

  /**
   * Parse the given version number as a constant or dot based version.
   * <p>This method allows to use {@code "LUCENE_X_Y"} constant names,
   * or version numbers in the format {@code "x.y.z"}.
   *
   * @lucene.internal
   */
  public static Version parseLeniently(String version) throws ParseException {
    String versionOrig = version;
    version = version.toUpperCase(Locale.ROOT);
    switch (version) {
      case "LATEST":
      case "LUCENE_CURRENT":
        return LATEST;
      default:
        version = version
                .replaceFirst("^LUCENE_(\\d+)_(\\d+)_(\\d+)$", "$1.$2.$3")
                .replaceFirst("^LUCENE_(\\d+)_(\\d+)$", "$1.$2.0")
                .replaceFirst("^LUCENE_(\\d)(\\d)$", "$1.$2.0");
        try {
          return parse(version);
        } catch (ParseException pe) {
          ParseException pe2 = new ParseException("failed to parse lenient version string \"" + versionOrig + "\": " + pe.getMessage(), 0);
          pe2.initCause(pe);
          throw pe2;
        }
    }
  }

  /** Returns a new version based on raw numbers
   *
   *  @lucene.internal */
  public static Version fromBits(int major, int minor, int bugfix) {
    return new Version(major, minor, bugfix);
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
    // NOTE: do not enforce major version so we remain future proof, except to
    // make sure it fits in the 8 bits we encode it into:
    if (major > 255 || major < 0) {
      throw new IllegalArgumentException("Illegal major version: " + major);
    }
    if (minor > 255 || minor < 0) {
      throw new IllegalArgumentException("Illegal minor version: " + minor);
    }
    if (bugfix > 255 || bugfix < 0) {
      throw new IllegalArgumentException("Illegal bugfix version: " + bugfix);
    }
    if (prerelease > 2 || prerelease < 0) {
      throw new IllegalArgumentException("Illegal prerelease version: " + prerelease);
    }
    if (prerelease != 0 && (minor != 0 || bugfix != 0)) {
      throw new IllegalArgumentException("Prerelease version only supported with major release (got prerelease: " + prerelease + ", minor: " + minor + ", bugfix: " + bugfix + ")");
    }

    encodedValue = major << 18 | minor << 10 | bugfix << 2 | prerelease;

    assert encodedIsValid();
  }

  /**
   * Returns true if this version is the same or after the version from the argument.
   */
  public boolean onOrAfter(Version other) {
    return encodedValue >= other.encodedValue;
  }

  @Override
  public String toString() {
    if (prerelease == 0) {
      return "" + major + "." + minor + "." + bugfix;
    }
    return "" + major + "." + minor + "." + bugfix + "." + prerelease;
  }

  @Override
  public boolean equals(Object o) {
    return o != null && o instanceof Version && ((Version)o).encodedValue == encodedValue;
  }

  // Used only by assert:
  private boolean encodedIsValid() {
    assert major == ((encodedValue >>> 18) & 0xFF);
    assert minor == ((encodedValue >>> 10) & 0xFF);
    assert bugfix == ((encodedValue >>> 2) & 0xFF);
    assert prerelease == (encodedValue & 0x03);
    return true;
  }

  @Override
  public int hashCode() {
    return encodedValue;
  }
}
