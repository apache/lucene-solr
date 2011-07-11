package org.apache.solr.common.luke;
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


/**
 *
 * @since solr 1.3
 */
public enum FieldFlag {
  INDEXED('I', "Indexed"), 
  TOKENIZED('T', "Tokenized"), 
  STORED('S', "Stored"), 
  MULTI_VALUED('M', "Multivalued"),
  TERM_VECTOR_STORED('V', "TermVector Stored"), 
  TERM_VECTOR_OFFSET('o', "Store Offset With TermVector"),
  TERM_VECTOR_POSITION('p', "Store Position With TermVector"),
  OMIT_NORMS('O', "Omit Norms"), 
  OMIT_TF('F', "Omit Tf"), 
  OMIT_POSITIONS('P', "Omit Positions"),
  LAZY('L', "Lazy"), 
  BINARY('B', "Binary"), 
  SORT_MISSING_FIRST('f', "Sort Missing First"), 
  SORT_MISSING_LAST('l', "Sort Missing Last");

  private final char abbreviation;
  private final String display;

  FieldFlag(char abbreviation, String display) {
    this.abbreviation = abbreviation;
    this.display = display;
    this.display.intern();//QUESTION:  Need we bother here?
  }

  public static FieldFlag getFlag(char abbrev){
    FieldFlag result = null;
    FieldFlag [] vals = FieldFlag.values();
    for (int i = 0; i < vals.length; i++) {
      if (vals[i].getAbbreviation() == abbrev){
        result = vals[i];
        break;
      }
    }
    return result;
  }

  public char getAbbreviation() {
    return abbreviation;
  }

  public String getDisplay() {
    return display;
  }
}
