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
 *
 * 
 * @lucene.internal
 */
public abstract class FieldProperties {

  // use a bitfield instead of many different boolean variables since
  // many of the variables are independent or semi-independent.

  // bit values for boolean field properties.
  protected final static int INDEXED             = 0x00000001;
  protected final static int TOKENIZED           = 0x00000002;
  protected final static int STORED              = 0x00000004;
  protected final static int BINARY              = 0x00000008;
  protected final static int OMIT_NORMS          = 0x00000010;
  protected final static int OMIT_TF_POSITIONS   = 0x00000020;
  protected final static int STORE_TERMVECTORS   = 0x00000040;
  protected final static int STORE_TERMPOSITIONS = 0x00000080;
  protected final static int STORE_TERMOFFSETS   = 0x00000100;


  protected final static int MULTIVALUED         = 0x00000200;
  protected final static int SORT_MISSING_FIRST  = 0x00000400;
  protected final static int SORT_MISSING_LAST   = 0x00000800;
  
  protected final static int REQUIRED            = 0x00001000;
  
  static final String[] propertyNames = {
          "indexed", "tokenized", "stored",
          "binary", "omitNorms", "omitTermFreqAndPositions",
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

  static int parseProperties(Map<String,String> properties, boolean which) {
    int props = 0;
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String val = entry.getValue();
      if(val == null) continue;
      if (Boolean.parseBoolean(val) == which) {
        props |= propertyNameToInt(entry.getKey());
      }
    }
    return props;
  }

}
