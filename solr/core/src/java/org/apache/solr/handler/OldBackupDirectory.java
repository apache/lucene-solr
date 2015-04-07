package org.apache.solr.handler;

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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class OldBackupDirectory implements Comparable<OldBackupDirectory> {
  File dir;
  Date timestamp;
  private  final Pattern dirNamePattern = Pattern.compile("^snapshot[.](.*)$");

  OldBackupDirectory(File dir) {
    if(dir.isDirectory()) {
      Matcher m = dirNamePattern.matcher(dir.getName());
      if(m.find()) {
        try {
          this.dir = dir;
          this.timestamp = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).parse(m.group(1));
        } catch(Exception e) {
          this.dir = null;
          this.timestamp = null;
        }
      }
    }
  }
  @Override
  public int compareTo(OldBackupDirectory that) {
    return that.timestamp.compareTo(this.timestamp);
  }
}
