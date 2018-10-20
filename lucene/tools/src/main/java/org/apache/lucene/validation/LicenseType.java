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
package org.apache.lucene.validation;


/**
 * A list of accepted licenses.  See also http://www.apache.org/legal/3party.html
 *
 **/
public enum LicenseType {
  ASL("Apache Software License 2.0", true),
  BSD("Berkeley Software Distribution", true),
  BSD_LIKE("BSD like license", true),//BSD like just means someone has taken the BSD license and put in their name, copyright, or it's a very similar license.
  CDDL("Common Development and Distribution License", false),
  CPL("Common Public License", true),
  EPL("Eclipse Public License Version 1.0", false),
  MIT("Massachusetts Institute of Tech. License", false),
  MPL("Mozilla Public License", false), //NOT SURE on the required notice
  PD("Public Domain", false),
  //SUNBCLA("Sun Binary Code License Agreement"),
  SUN("Sun Open Source License", false),
  COMPOUND("Compound license (see NOTICE).", true),
  FAKE("FAKE license - not needed", false);

  private String display;
  private boolean noticeRequired;

  LicenseType(String display, boolean noticeRequired) {
    this.display = display;
    this.noticeRequired = noticeRequired;
  }

  public boolean isNoticeRequired() {
    return noticeRequired;
  }

  public String getDisplay() {
    return display;
  }

  public String toString() {
    return "LicenseType{" +
            "display='" + display + '\'' +
            '}';
  }

  /**
   * Expected license file suffix for a given license type.
   */
  public String licenseFileSuffix() {
    return "-LICENSE-" + this.name() + ".txt";
  }

  /**
   * Expected notice file suffix for a given license type.
   */
  public String noticeFileSuffix() {
    return "-NOTICE.txt";
  }
}

