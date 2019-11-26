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
package org.apache.solr.schema;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * 
 * @lucene.internal
 */
public abstract class FieldProperties {

  // use a bitfield instead of many different boolean variables since
  // many of the variables are independent or semi-independent.

  // bit values for boolean field properties.
  protected final static int INDEXED             = 0b1;
  protected final static int TOKENIZED           = 0b10;
  protected final static int STORED              = 0b100;
  protected final static int BINARY              = 0b1000;
  protected final static int OMIT_NORMS          = 0b10000;
  protected final static int OMIT_TF_POSITIONS   = 0b100000;
  protected final static int STORE_TERMVECTORS   = 0b1000000;
  protected final static int STORE_TERMPOSITIONS = 0b10000000;
  protected final static int STORE_TERMOFFSETS   = 0b100000000;


  protected final static int MULTIVALUED         = 0b1000000000;
  protected final static int SORT_MISSING_FIRST  = 0b10000000000;
  protected final static int SORT_MISSING_LAST   = 0b100000000000;
  
  protected final static int REQUIRED            = 0b1000000000000;
  protected final static int OMIT_POSITIONS      = 0b10000000000000;

  protected final static int STORE_OFFSETS       = 0b100000000000000;
  protected final static int DOC_VALUES          = 0b1000000000000000;

  protected final static int STORE_TERMPAYLOADS  = 0b10000000000000000;
  protected final static int USE_DOCVALUES_AS_STORED  = 0b100000000000000000;
  protected final static int LARGE_FIELD         = 0b1000000000000000000;
  protected final static int UNINVERTIBLE        = 0b10000000000000000000;

  static final String[] propertyNames = {
          "indexed", "tokenized", "stored",
          "binary", "omitNorms", "omitTermFreqAndPositions",
          "termVectors", "termPositions", "termOffsets",
          "multiValued",
          "sortMissingFirst","sortMissingLast","required", "omitPositions",
          "storeOffsetsWithPositions", "docValues", "termPayloads", "useDocValuesAsStored", "large",
          "uninvertible"
  };

  static final Map<String,Integer> propertyMap = new HashMap<>();
  static {
    for (String prop : propertyNames) {
      propertyMap.put(prop, propertyNameToInt(prop, true));
    }
  }

  static final String POSTINGS_FORMAT = "postingsFormat";
  static final String DOC_VALUES_FORMAT = "docValuesFormat";

  /** Returns the symbolic name for the property. */
  static String getPropertyName(int property) {
    return propertyNames[ Integer.numberOfTrailingZeros(property) ];
  }

  static int propertyNameToInt(String name, boolean failOnError) {
    for (int i=0; i<propertyNames.length; i++) {
      if (propertyNames[i].equals(name)) {
        return 1 << i;
      }
    }
    if (failOnError && !isPropertyIgnored(name)) {
      throw new IllegalArgumentException("Invalid field property: " + name);
    } else {
      return 0;
    }
  }

  private static boolean isPropertyIgnored(String name) {
    return name.equals("default") || name.equals(POSTINGS_FORMAT) || name.equals(DOC_VALUES_FORMAT);
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

  static int parseProperties(Map<String,?> properties, boolean which, boolean failOnError) {
    int props = 0;
    for (Map.Entry<String,?> entry : properties.entrySet()) {
      Object val = entry.getValue();
      if(val == null) continue;
      boolean boolVal = val instanceof Boolean ? (Boolean)val : Boolean.parseBoolean(val.toString());
      if (boolVal == which) {
        props |= propertyNameToInt(entry.getKey(), failOnError);
      }
    }
    return props;
  }
}
