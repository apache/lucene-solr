package org.apache.lucene.validation;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Mapper;
import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.resources.FileResource;
import org.apache.tools.ant.types.resources.Resources;
import org.apache.tools.ant.util.FileNameMapper;

/**
 * An ANT task that verifies if JAR file have associated <tt>LICENSE</tt>,
 * <tt>NOTICE</tt>, and <tt>sha1</tt> files. 
 */
public class LicenseCheckTask extends Task {

  public final static String CHECKSUM_TYPE = "sha1";
  private static final int CHECKSUM_BUFFER_SIZE = 8 * 1024;
  private static final int CHECKSUM_BYTE_MASK = 0xFF;

  /**
   * All JAR files to check.
   */
  private Resources jarResources = new Resources();

  /**
   * License file mapper.
   */
  private FileNameMapper licenseMapper;

  /**
   * A logging level associated with verbose logging.
   */
  private int verboseLevel = Project.MSG_VERBOSE;
  
  /**
   * Failure flag.
   */
  private boolean failures;

  /**
   * Adds a set of JAR resources to check.
   */
  public void add(ResourceCollection rc) {
    jarResources.add(rc);
  }
  
  /**
   * Adds a license mapper.
   */
  public void addConfiguredLicenseMapper(Mapper mapper) {
    if (licenseMapper != null) {
      throw new BuildException("Only one license mapper is allowed.");
    }
    this.licenseMapper = mapper.getImplementation();
  }

  public void setVerbose(boolean verbose) {
    verboseLevel = (verbose ? Project.MSG_INFO : Project.MSG_VERBOSE);
  }

  /**
   * Execute the task.
   */
  @Override
  public void execute() throws BuildException {
    if (licenseMapper == null) {
      throw new BuildException("Expected an embedded <licenseMapper>.");
    }

    jarResources.setProject(getProject());
    processJars();

    if (failures) {
      throw new BuildException("License check failed. Check the logs.");
    }
  }

  /**
   * Process all JARs.
   */
  private void processJars() {
    log("Starting scan.", verboseLevel);
    long start = System.currentTimeMillis();

    @SuppressWarnings("unchecked")
    Iterator<Resource> iter = (Iterator<Resource>) jarResources.iterator();
    int checked = 0;
    int errors = 0;
    while (iter.hasNext()) {
      final Resource r = iter.next();
      if (!r.isExists()) { 
        throw new BuildException("JAR resource does not exist: " + r.getName());
      }
      if (!(r instanceof FileResource)) {
        throw new BuildException("Only filesystem resource are supported: " + r.getName()
            + ", was: " + r.getClass().getName());
      }

      File jarFile = ((FileResource) r).getFile();
      if (! checkJarFile(jarFile) ) {
        errors++;
      }
      checked++;
    }

    log(String.format(Locale.ENGLISH, 
        "Scanned %d JAR file(s) for licenses (in %.2fs.), %d error(s).",
        checked, (System.currentTimeMillis() - start) / 1000.0, errors),
        errors > 0 ? Project.MSG_ERR : Project.MSG_INFO);
  }

  /**
   * Check a single JAR file.
   */
  private boolean checkJarFile(File jarFile) {
    log("Scanning: " + jarFile.getPath(), verboseLevel);

    // validate the jar matches against our expected hash
    final File checksumFile = new File(jarFile.getParent(), 
                                       jarFile.getName() + "." + CHECKSUM_TYPE);
    if (! (checksumFile.exists() && checksumFile.canRead()) ) {
      log("MISSING " +CHECKSUM_TYPE+ " checksum file for: " + jarFile.getPath(), Project.MSG_ERR);
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
          throw new BuildException("IO error computing checksum of file: " + jarFile, ioe);
        }
        final byte[] checksumBytes = md.digest();
        final String checksum = createChecksumString(checksumBytes);
        if ( ! checksum.equals(expectedChecksum) ) {
          log("CHECKSUM FAILED for " + jarFile.getPath() + 
              " (expected: \"" + expectedChecksum + "\" was: \"" + checksum + "\")", 
              Project.MSG_ERR);
          this.failures = true;
          return false;
        }

      } catch (NoSuchAlgorithmException ae) {
        throw new BuildException("Digest type " + CHECKSUM_TYPE + " not supported by your JVM", ae);
      }
    }
    
    // Get the expected license path base from the mapper and search for license files.
    Map<File, LicenseType> foundLicenses = new LinkedHashMap<File, LicenseType>();
    List<File> expectedLocations = new ArrayList<File>();
outer:
    for (String mappedPath : licenseMapper.mapFileName(jarFile.getPath())) {
      for (LicenseType licenseType : LicenseType.values()) {
        File licensePath = new File(mappedPath + licenseType.licenseFileSuffix());
        if (licensePath.exists()) {
          foundLicenses.put(licensePath, licenseType);
          log(" FOUND " + licenseType.name() + " license at " + licensePath.getPath(), 
              verboseLevel);
          // We could continue scanning here to detect duplicate associations?
          break outer;
        } else {
          expectedLocations.add(licensePath);
        }
      }
    }

    // Check for NOTICE files.
    for (Map.Entry<File, LicenseType> e : foundLicenses.entrySet()) {
      LicenseType license = e.getValue();
      String licensePath = e.getKey().getAbsolutePath();
      String baseName = licensePath.substring(
          0, licensePath.length() - license.licenseFileSuffix().length());
      File noticeFile = new File(baseName + license.noticeFileSuffix());

      if (noticeFile.exists()) {
        log(" FOUND NOTICE file at " + noticeFile.getAbsolutePath(), verboseLevel);
      } else {
        if (license.isNoticeRequired()) {
            this.failures = true;
            log("MISSING NOTICE for the license file:\n  "
                + licensePath + "\n  Expected location below:\n  "
                + noticeFile.getAbsolutePath(), Project.MSG_ERR);
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
      log(message.toString(), Project.MSG_ERR);
      return false;
    }

    return true;
  }

  private static final String createChecksumString(byte[] digest) {
    StringBuilder checksum = new StringBuilder();
    for (int i = 0; i < digest.length; i++) {
      checksum.append(String.format(Locale.ENGLISH, "%02x", 
                                    CHECKSUM_BYTE_MASK & digest[i]));
    }
    return checksum.toString();
  }
  private static final String readChecksumFile(File f) {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader
                                  (new FileInputStream(f), "UTF-8"));
      try {
        String checksum = reader.readLine();
        if (null == checksum || 0 == checksum.length()) {
          throw new BuildException("Failed to find checksum in file: " + f);
        }
        return checksum;
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new BuildException("IO error reading checksum file: " + f, e);
    }
  }

}
