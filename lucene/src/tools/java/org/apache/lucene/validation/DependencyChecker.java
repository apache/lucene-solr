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
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 *
 **/
public class DependencyChecker {
  private static Set<String> excludes = new HashSet<String>();
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  static {
    //Collections.addAll(excludes, );
  }

  public static void main(String[] args) throws IOException {
    String dumpFile = null;
    List<String> dirs = new ArrayList<String>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].equalsIgnoreCase("--dump") || args[i].equalsIgnoreCase("-d")) {
        dumpFile = args[++i];

      } else if (args[i].equalsIgnoreCase("--check") || args[i].equalsIgnoreCase("-c")) {
        dirs.add(args[++i]);
      }
    }


    FileWriter writer = null;
    boolean dump = false;
    if (dumpFile != null) {
      File out = new File(dumpFile);
      System.out.println("Dumping to " + out);
      writer = new FileWriter(out);
      dump = true;
    }
    //TODO: put in NOTICE checks
    for (String checkDir : dirs) {
      File dir = new File(checkDir);
      if (dir.exists()) {
        System.out.println("----------------------");
        System.out.println("Starting on dir: " + dir);
        int numFailed = 0;
        File[] list = dir.listFiles();
        File[] licFiles = dir.listFiles(new FileFilter() {
          public boolean accept(File file) {
            return file.getName().indexOf("-LICENSE") != -1 && file.getName().endsWith(".txt");//check for a consistent end, so that we aren't fooled by emacs ~ files or other temp files
          }
        });
        File[] noticeFiles = dir.listFiles(new FileFilter() {
          public boolean accept(File file) {
            return file.getName().indexOf("-NOTICE") != -1 && file.getName().endsWith(".txt");
          }
        });
        File[] jarFiles = dir.listFiles(new FileFilter() {
          public boolean accept(File file) {
            return file.getName().endsWith(".jar");
          }
        });
        if (licFiles.length == 0 && jarFiles.length != 0) {
          System.out.println("No license files found: " + dir);
          numFailed++;
        }
        if (jarFiles.length != licFiles.length) {
          System.out.println("WARNING: There are missing LICENSE files in: " + dir + " Jar file count: " + jarFiles.length + " License Count: " + licFiles.length);
          printDiffs(jarFiles, licFiles);
          numFailed++;
        }
        if (jarFiles.length != noticeFiles.length) {
          System.out.println("WARNING: There may be missing NOTICE files in: " + dir + ".  Note, not all files require a NOTICE. Jar file count: " + jarFiles.length + " Notice Count: " + noticeFiles.length);
          //printDiffs(jarFiles, noticeFiles);
        }
        Map<String, UpdateableInt> licenseNames = new HashMap<String, UpdateableInt>();
        for (int i = 0; i < licFiles.length; i++) {
          licenseNames.put(licFiles[i].getName(), new UpdateableInt());
        }
        Map<String, UpdateableInt> noticeNames = new HashMap<String, UpdateableInt>();
        for (int i = 0; i < noticeFiles.length; i++) {
          noticeNames.put(noticeFiles[i].getName(), new UpdateableInt());
        }


        for (int i = 0; i < list.length; i++) {
          File file = list[i];
          String fileName = file.getName();
          if (fileName.endsWith(".jar") && excludes.contains(fileName) == false) {
            File licFile = getLicenseFile(file, licenseNames);
            if (licFile != null && licFile.exists()) {
              String licName = licFile.getName();
              LicenseType[] types = getLicenseTypes(licName);
              if (types != null && types.length > 0) {
                for (int j = 0; j < types.length; j++) {
                  LicenseType type = types[j];
                  if (dump == true) {
                    writer.write(file.getName() + "," + type.getDisplay() + LINE_SEPARATOR);
                  }
                  if (type.isNoticeRequired()) {
                    File noticeFile = getNoticeFile(file, noticeNames);
                    if (noticeFile != null && noticeFile.exists()) {

                    } else {
                      System.out.println("!!!!!! Missing NOTICE file for " + file + " and license type: " + type.getDisplay());
                      if (dump) {
                        writer.write("Missing NOTICE file for " + file + LINE_SEPARATOR);
                      }
                      numFailed++;
                    }
                  }
                }
              } else {
                System.out.println("!!!!!! Couldn't determine license type for file: " + file);
                if (dump == true) {
                  writer.write("Invalid license for file: " + file + LINE_SEPARATOR);
                }
                numFailed++;
              }
            } else {
              System.out.println("!!!!!!! Couldn't get license file for " + file);
              if (dump == true) {
                writer.write("Couldn't get license file for " + file + LINE_SEPARATOR);
              }
              numFailed++;
            }
          }
        }
        if (dump == true) {
          writer.write(LINE_SEPARATOR + LINE_SEPARATOR);
          writer.write("Other Licenses (installer, javascript, etc." + LINE_SEPARATOR);
        }

        if (dump == true) {
          for (Map.Entry<String, UpdateableInt> entry : licenseNames.entrySet()) {
            if (entry.getValue().theInt == 0) {
              LicenseType[] types = getLicenseTypes(entry.getKey());
              if (types != null && types.length > 0) {
                for (int i = 0; i < types.length; i++) {
                  writer.write(entry.getKey() + "," + types[i].getDisplay() + LINE_SEPARATOR);
                }
              } else {
                System.out.println("Couldn't determine license for: " + entry.getKey());
              }
            }
          }
        }
        if (writer != null) {
          writer.close();
        }
        if (numFailed > 0) {
          System.out.println("At least one file does not have a license, or it's license name is not in the proper format.  See the logs.");
          System.exit(-1);
        } else {
          System.out.println("Found a license for every file in " + dir);
        }
      } else {
        System.out.println("Could not find directory:" + dir);
      }
    }
  }


  /**
   * Sort the two lists and then print them out for visual comparison
   *
   * @param left
   * @param right
   */

  private static void printDiffs(File[] left, File[] right) {
    Arrays.sort(left);
    Arrays.sort(right);
    System.out.println("Left\t\t\tRight");
    System.out.println("----------------");
    StringBuilder bldr = new StringBuilder();
    int i = 0;
    for (; i < left.length; i++) {
      bldr.append(left[i]).append("\t\t\t");
      if (i < right.length) {
        bldr.append(right[i]);
      }
      bldr.append(LINE_SEPARATOR);
    }
    if (i < right.length) {
      for (; i < right.length; i++) {
        bldr.append("--- N/A ---\t\t\t").append(right[i]).append(LINE_SEPARATOR);
      }
    }
    System.out.println(bldr.toString());
    System.out.println("----------------");
  }

  private static LicenseType[] getLicenseTypes(String licName) {
    LicenseType[] result = new LicenseType[0];
    int idx = licName.lastIndexOf("-");
    if (idx != -1) {
      String licAbbrev = licName.substring(idx + 1, licName.length() - ".txt".length());
      String[] lics = licAbbrev.split("__");
      result = new LicenseType[lics.length];
      for (int j = 0; j < lics.length; j++) {
        try {
          result[j] = LicenseType.valueOf(lics[j].toUpperCase());
        } catch (IllegalArgumentException e) {
          System.out.println("Invalid license: " + lics[j].toUpperCase() + " for " + licName);
        }
      }
    }
    return result;
  }

  private static File getLicenseFile(File file, Map<String, UpdateableInt> licenseNames) {
    File result = null;
    String filename = file.getName();
    int length = 0;
    for (String licName : licenseNames.keySet()) {
      String prefix = licName.substring(0, licName.indexOf("-LICENSE"));
      String name = null;
      //System.out.println("prefix: " + prefix + " lic name: " + licName);
      if (filename.toLowerCase().startsWith(prefix.toLowerCase())) {
        result = new File(file.getParentFile(), licName);
        UpdateableInt ui = licenseNames.get(licName);
        ui.theInt++;
      } else {
      }

    }
    //System.out.println("License File: " + result + " for file: " + file);

    return result;
  }

  private static File getNoticeFile(File file, Map<String, UpdateableInt> noticeNames) {
    File result = null;
    String filename = file.getName();
    int length = 0;
    for (String noticeName : noticeNames.keySet()) {
      String prefix = noticeName.substring(0, noticeName.indexOf("-NOTICE"));
      String name = null;
      //System.out.println("prefix: " + prefix + " lic name: " + licName);
      if (filename.toLowerCase().startsWith(prefix.toLowerCase())) {
        result = new File(file.getParentFile(), noticeName);
        UpdateableInt ui = noticeNames.get(noticeName);
        ui.theInt++;
      } else {
      }

    }
    //System.out.println("License File: " + result + " for file: " + file);

    return result;
  }

}

class UpdateableInt {
  public int theInt;
}
