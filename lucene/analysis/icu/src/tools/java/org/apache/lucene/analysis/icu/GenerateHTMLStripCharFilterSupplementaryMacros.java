package org.apache.lucene.analysis.icu;

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

import java.text.DateFormat;
import java.util.*;

import com.ibm.icu.text.UnicodeSet;
import com.ibm.icu.text.UnicodeSetIterator;
import com.ibm.icu.util.VersionInfo;

/** creates a macro to augment jflex's unicode support for > BMP */
public class GenerateHTMLStripCharFilterSupplementaryMacros {
  private static final UnicodeSet BMP = new UnicodeSet("[\u0000-\uFFFF]");
  private static final String NL = System.getProperty("line.separator");
  private static final DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance
      (DateFormat.FULL, DateFormat.FULL, Locale.ROOT);
  static {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  private static final String APACHE_LICENSE
      = "/*" + NL
      + " * Copyright 2010 The Apache Software Foundation." + NL
      + " *" + NL
      + " * Licensed under the Apache License, Version 2.0 (the \"License\");" + NL
      + " * you may not use this file except in compliance with the License." + NL
      + " * You may obtain a copy of the License at" + NL
      + " *" + NL
      + " *      http://www.apache.org/licenses/LICENSE-2.0" + NL
      + " *" + NL
      + " * Unless required by applicable law or agreed to in writing, software" + NL
      + " * distributed under the License is distributed on an \"AS IS\" BASIS," + NL
      + " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied." + NL
      + " * See the License for the specific language governing permissions and" + NL
      + " * limitations under the License." + NL
      + " */" + NL + NL;


  public static void main(String args[]) {
    outputHeader();
    outputMacro("ID_Start_Supp", "[:ID_Start:]");
    outputMacro("ID_Continue_Supp", "[:ID_Continue:]");
  }

  static void outputHeader() {
    System.out.print(APACHE_LICENSE);
    System.out.print("// Generated using ICU4J " + VersionInfo.ICU_VERSION.toString() + " on ");
    System.out.println(DATE_FORMAT.format(new Date()));
    System.out.println("// by " + GenerateHTMLStripCharFilterSupplementaryMacros.class.getName());
    System.out.print(NL + NL);
  }

  // we have to carefully output the possibilities as compact utf-16
  // range expressions, or jflex will OOM!
  static void outputMacro(String name, String pattern) {
    UnicodeSet set = new UnicodeSet(pattern);
    set.removeAll(BMP);
    System.out.println(name + " = (");
    // if the set is empty, we have to do this or jflex will barf
    if (set.isEmpty()) {
      System.out.println("\t  []");
    }

    HashMap<Character,UnicodeSet> utf16ByLead = new HashMap<Character,UnicodeSet>();
    for (UnicodeSetIterator it = new UnicodeSetIterator(set); it.next();) {
      char utf16[] = Character.toChars(it.codepoint);
      UnicodeSet trails = utf16ByLead.get(utf16[0]);
      if (trails == null) {
        trails = new UnicodeSet();
        utf16ByLead.put(utf16[0], trails);
      }
      trails.add(utf16[1]);
    }
    
    Map<String,UnicodeSet> utf16ByTrail = new HashMap<String,UnicodeSet>();
    for (Map.Entry<Character,UnicodeSet> entry : utf16ByLead.entrySet()) {
      String trail = entry.getValue().getRegexEquivalent();
      UnicodeSet leads = utf16ByTrail.get(trail);
      if (leads == null) {
        leads = new UnicodeSet();
        utf16ByTrail.put(trail, leads);
      }
      leads.add(entry.getKey());
    }

    boolean isFirst = true;
    for (Map.Entry<String,UnicodeSet> entry : utf16ByTrail.entrySet()) {
      System.out.print( isFirst ? "\t  " : "\t| ");
      isFirst = false;
      System.out.println(entry.getValue().getRegexEquivalent() + entry.getKey());
    }
    System.out.println(")");
  }
}
