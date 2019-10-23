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
package org.apache.lucene.gradle;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.logging.LogLevel;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class LicenseCheckTask extends DefaultTask {
  private final static String CHECKSUM_TYPE = "sha1";
  private static final int CHECKSUM_BUFFER_SIZE = 8 * 1024;
  private static final int CHECKSUM_BYTE_MASK = 0xFF;
  private static final String FAILURE_MESSAGE = "License check failed. Check the logs.\n"
      + "If you recently modified version.props or in some cases a module's build.gradle,\n"
      + "make sure you run \"gw jarChecksums\" to regenerate checksum files.";
  private static final Map<Pattern, String> REPLACE_PATTERNS = new HashMap<>();
  static {
    REPLACE_PATTERNS.put(Pattern.compile("jetty([^/]+)$"), "jetty");
    REPLACE_PATTERNS.put(Pattern.compile("asm([^/]+)$"), "asm");
    REPLACE_PATTERNS.put(Pattern.compile("slf4j-([^/]+)$"), "slf4j");
    REPLACE_PATTERNS.put(Pattern.compile("(?:bcmail|bcprov)-([^\\.]+)$"), "$1");
  }

  private Pattern skipRegexChecksum;
  private boolean skipSnapshotsChecksum;
  private boolean skipChecksum;


  /**
   * Directory containing licenses
   */
  private File licenseDirectory;

  /**
   * Failure flag.
   */
  private boolean failures;

  @Inject
  public LicenseCheckTask(File licenseDirectory) {
    setGroup("Verification");
    setDescription("Check licenses for dependencies.");
    this.licenseDirectory = licenseDirectory;
    doLast(task -> {
      Set<ResolvedArtifact> deps = new HashSet<>();
      getProject().getSubprojects().forEach(project -> {
        project.getConfigurations().forEach(config -> {
          if (config.isCanBeResolved()) {
            deps.addAll(config.getResolvedConfiguration().getResolvedArtifacts());
          }
        });
      });
      deps.removeIf(artifact -> {
        String groupName = artifact.getModuleVersion().getId().getGroup();
        return "org.apache.lucene".equals(groupName) || "org.apache.solr".equals(groupName);
      });

      doCheck(deps);
    });
  }

  public void setSkipSnapshotsChecksum(boolean skipSnapshotsChecksum) {
    this.skipSnapshotsChecksum = skipSnapshotsChecksum;
  }

  public void setSkipChecksum(boolean skipChecksum) {
    this.skipChecksum = skipChecksum;
  }

  public void setSkipRegexChecksum(String skipRegexChecksum) {
    try {
      if (skipRegexChecksum != null && skipRegexChecksum.length() > 0) {
        this.skipRegexChecksum = Pattern.compile(skipRegexChecksum);
      }
    } catch (PatternSyntaxException e) {
      throw new GradleException("Unable to compile skipRegexChecksum pattern.  Reason: "
          + e.getMessage() + " " + skipRegexChecksum, e);
    }
  }

  /**
   * Execute the task.
   */
  public void doCheck(Set<ResolvedArtifact> jars) {
    if (skipChecksum) {
      getLogger().info("Skipping checksum verification for dependencies");
    } else {
      if (skipSnapshotsChecksum) {
        getLogger().info("Skipping checksum for SNAPSHOT dependencies");
      }

      if (skipRegexChecksum != null) {
        getLogger().info("Skipping checksum for dependencies matching regex: " + skipRegexChecksum.pattern());
      }
    }

    processJars(jars);

    if (failures) {
      throw new GradleException(FAILURE_MESSAGE);
    }
  }

  /**
   * Process all JARs.
   */
  private void processJars(Set<ResolvedArtifact> jars) {
    long start = System.currentTimeMillis();
    int errors = 0;
    int checked = 0;
    for (ResolvedArtifact artifact : jars) {
      if (!checkJarFile(artifact)) {
        errors++;
      }
      checked++;
    }

    getLogger().log(errors > 0 ? LogLevel.ERROR : LogLevel.INFO, String.format(Locale.ROOT,
        "Scanned %d JAR file(s) for licenses (in %.2fs.), %d error(s).",
        checked, (System.currentTimeMillis() - start) / 1000.0, errors));
  }

  /**
   * Check a single JAR file.
   */
  private boolean checkJarFile(ResolvedArtifact artifact) {
    File jarFile = artifact.getFile();
    getLogger().debug("Scanning: {}", jarFile.getPath());

    if (!skipChecksum) {
      boolean skipDueToSnapshot = skipSnapshotsChecksum && jarFile.getName().contains("-SNAPSHOT");
      if (!skipDueToSnapshot && !matchesRegexChecksum(jarFile, skipRegexChecksum)) {
        // validate the jar matches against our expected hash
        final File checksumFile = new File(licenseDirectory, jarFile.getName()
            + "." + CHECKSUM_TYPE);
        if (!(checksumFile.exists() && checksumFile.canRead())) {
          getLogger().error("MISSING {} checksum file for: {}", CHECKSUM_TYPE, jarFile.getPath());
          this.failures = true;
          return false;
        } else {
          final String expectedChecksum = readChecksumFile(checksumFile);
          try {
            final MessageDigest md = MessageDigest.getInstance(CHECKSUM_TYPE);
            byte[] buf = new byte[CHECKSUM_BUFFER_SIZE];
            try {
              FileInputStream fis = new FileInputStream(jarFile);
              try {
                DigestInputStream dis = new DigestInputStream(fis, md);
                try {
                  while (dis.read(buf, 0, CHECKSUM_BUFFER_SIZE) != -1) {
                    // NOOP
                  }
                } finally {
                  dis.close();
                }
              } finally {
                fis.close();
              }
            } catch (IOException ioe) {
              throw new GradleException("IO error computing checksum of file: "
                  + jarFile, ioe);
            }
            final byte[] checksumBytes = md.digest();
            final String checksum = createChecksumString(checksumBytes);
            if (!checksum.equals(expectedChecksum)) {
              getLogger().error("CHECKSUM FAILED for " + jarFile.getPath() + " (expected: \""
                  + expectedChecksum + "\" was: \"" + checksum + "\")");
              this.failures = true;
              return false;
            }

          } catch (NoSuchAlgorithmException ae) {
            throw new GradleException("Digest type " + CHECKSUM_TYPE
                + " not supported by your JVM", ae);
          }
        }
      } else if (skipDueToSnapshot) {
        getLogger().info("Skipping jar because it is a SNAPSHOT : "
            + jarFile.getAbsolutePath());
      } else {
        getLogger().info("Skipping jar because it matches regex pattern: {} pattern:{}",
            jarFile.getAbsolutePath(), skipRegexChecksum.pattern());
      }
    }

    // Get the expected license path base from the mapper and search for license files.
    Map<File, LicenseType> foundLicenses = new LinkedHashMap<>();
    List<File> expectedLocations = new ArrayList<>();
    outer:
    for (LicenseType licenseType : LicenseType.values()) {
      String artifactName = artifact.getModuleVersion().getId().getName();
      //System.out.println("artifact name:" + artifactName);
      for (Map.Entry<Pattern, String> entry : REPLACE_PATTERNS.entrySet()) {
        //System.out.println("replace with:" + entry.getValue());
        artifactName = entry.getKey().matcher(artifactName).replaceAll(entry.getValue());
      }
      File licensePath = new File(licenseDirectory,
          artifactName + licenseType.licenseFileSuffix());
      if (licensePath.exists()) {
        foundLicenses.put(licensePath, licenseType);
        getLogger().debug(" FOUND " + licenseType.name() + " license at " + licensePath.getPath());
        // We could continue scanning here to detect duplicate associations?
        break outer;
      } else {
        expectedLocations.add(licensePath);
      }
    }

    // Check for NOTICE files.
    for (Map.Entry<File, LicenseType> e : foundLicenses.entrySet()) {
      LicenseType license = e.getValue();
      String licensePath = e.getKey().getName();
      String baseName = licensePath.substring(
          0, licensePath.length() - license.licenseFileSuffix().length());
      File noticeFile = new File(licenseDirectory, baseName + license.noticeFileSuffix());

      if (noticeFile.exists()) {
        getLogger().debug(" FOUND NOTICE file at " + noticeFile.getAbsolutePath());
      } else {
        if (license.isNoticeRequired()) {
          this.failures = true;
          getLogger().error("MISSING NOTICE for the license file:\n  "
              + licensePath + "\n  Expected location below:\n  "
              + noticeFile.getAbsolutePath());
        }
      }
    }

    // In case there is something missing, complain.
    if (foundLicenses.isEmpty()) {
      this.failures = true;
      StringBuilder message = new StringBuilder();
      message.append(
          "MISSING LICENSE for the following file:\n  " + jarFile.getAbsolutePath()
              + "\n  Expected locations below:\n");
      for (File location : expectedLocations) {
        message.append("  => ").append(location.getAbsolutePath()).append("\n");
      }
      getLogger().error(message.toString());
      return false;
    }

    return true;
  }

  private static String createChecksumString(byte[] digest) {
    StringBuilder checksum = new StringBuilder();
    for (byte aDigest : digest) {
      checksum.append(String.format(Locale.ROOT, "%02x",
          CHECKSUM_BYTE_MASK & aDigest));
    }
    return checksum.toString();
  }

  private static String readChecksumFile(File f) {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader
          (new FileInputStream(f), StandardCharsets.UTF_8));
      try {
        String checksum = reader.readLine();
        if (null == checksum || 0 == checksum.length()) {
          throw new GradleException("Failed to find checksum in file: " + f);
        }
        return checksum;
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new GradleException("IO error reading checksum file: " + f, e);
    }
  }

  private static final boolean matchesRegexChecksum(File jarFile, Pattern skipRegexChecksum) {
    if (skipRegexChecksum == null) {
      return false;
    }
    Matcher m = skipRegexChecksum.matcher(jarFile.getName());
    return m.matches();
  }

}
