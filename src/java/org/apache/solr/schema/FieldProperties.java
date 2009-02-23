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

package org.apache.solr.schema;

import java.util.Map;
import java.util.HashMap;

/**
 * @version $Id$
 */
abstract class FieldProperties {

  // use a bitfield instead of many different boolean variables since
  // many of the variables are independent or semi-independent.

  // bit values for boolean field properties.
  final static int INDEXED             = 0x00000001;
  final static int TOKENIZED           = 0x00000002;
  final static int STORED              = 0x00000004;
  final static int BINARY              = 0x00000008;
  final static int COMPRESSED          = 0x00000010;
  final static int OMIT_NORMS          = 0x00000020;
  final static int OMIT_TF             = 0x00000040;
  final static int STORE_TERMVECTORS   = 0x00000080;
  final static int STORE_TERMPOSITIONS = 0x00000100;
  final static int STORE_TERMOFFSETS   = 0x00000200;


  final static int MULTIVALUED         = 0x00000400;
  final static int SORT_MISSING_FIRST  = 0x00000800;
  final static int SORT_MISSING_LAST   = 0x00001000;
  
  final static int REQUIRED            = 0x00002000;
  
  static final String[] propertyNames = {
          "indexed", "tokenized", "stored",
          "binary", "compressed", "omitNorms", "omitTf",
          "termVectors", "termPositions", "termOffsets",
          "multiValued",
          "sortMissingFirst","sortMissingLast","required"
  };

  static final Map<String,Integer> propertyMap = new HashMap<String,Integer>();
  static {
    for (String prop : propertyNames) {
      propertyMap.put(prop, propertyNameToInt(prop));
    }
  }


  /** Returns the symbolic name for the property. */
  static String getPropertyName(int property) {
    return propertyNames[ Integer.numberOfTrailingZeros(property) ];
  }

  static int propertyNameToInt(String name) {
    for (int i=0; i<propertyNames.length; i++) {
      if (propertyNames[i].equals(name)) {
        return 1 << i;
      }
    }
    return 0;
  }


  static String propertiesToString(int properties) {
    StringBuilder sb = new StringBuilder();
    boolean first=true;
    while (properties != 0) {
      if (!first) sb.append(',');
      first=false;
      int bitpos = Integer.numberOfTrailingZeros(properties);
      sb.append(getPropertyName(1 << bitpos));
      properties &= ~(1<<bitpos);  // clear that bit position
    }
    return sb.toString();
  }

  static boolean on(int bitfield, int props) {
    return (bitfield & props) != 0;
  }

  static boolean off(int bitfield, int props) {
    return (bitfield & props) == 0;
  }

  /***
  static int normalize(int properties) {
    int p = properties;
    if (on(p,TOKENIZED) && off(p,INDEXED)) {
      throw new RuntimeException("field must be indexed to be tokenized.");
    }

    if (on(p,STORE_TERMPOSITIONS)) p|=STORE_TERMVECTORS;
    if (on(p,STORE_TERMOFFSETS)) p|=STORE_TERMVECTORS;
    if (on(p,STORE_TERMOFFSETS) && off(p,INDEXED)) {
      throw new RuntimeException("field must be indexed to store term vectors.");
    }

    if (on(p,OMIT_NORMS) && off(p,INDEXED)) {
      throw new RuntimeException("field must be indexed for norms to be omitted.");
    }

    if (on(p,SORT_MISSING_FIRST) && on(p,SORT_MISSING_LAST)) {
      throw new RuntimeException("conflicting options sortMissingFirst,sortMissingLast.");
    }

    if ((on(p,SORT_MISSING_FIRST) || on(p,SORT_MISSING_LAST)) && off(p,INDEXED)) {
      throw new RuntimeException("field must be indexed to be sorted.");
    }

    if ((on(p,BINARY) || on(p,COMPRESSED)) && off(p,STORED)) {
      throw new RuntimeException("field must be stored for compressed or binary options.");
    }

    return p;
  }
  ***/


  static int parseProperties(Map<String,String> properties, boolean which) {
    int props = 0;
    for (String prop : properties.keySet()) {
      if (propertyMap.get(prop)==null) continue;
      String val = properties.get(prop);
      if (Boolean.parseBoolean(val) == which) {
        props |= propertyNameToInt(prop);
      }
    }
    return props;
  }

}
