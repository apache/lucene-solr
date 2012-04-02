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

package org.apache.noggit;

import java.util.HashMap;

/**
 * @author yonik
 * @version $Id$
 */

     

// how to couple with JSONParser to allow streaming large values from input to
// output?
// IDEA 1) have JSONParser.getString(JSONWriter out)?
// IDEA 2) have an output CharArr that acts as a filter to escape data

// IDEA: a subclass of JSONWriter could provide more state and stricter checking

public class JSONWriter extends TextWriter {
  int level;
  boolean doIndent;
  final CharArr out;

  JSONWriter(CharArr out) {
    this.out = out;
  }

  public void writeNull() {
    JSONUtil.writeNull(out);
  }

  public void writeString(CharSequence str) {
    JSONUtil.writeString(str,0,str.length(),out);
  }

  public void writeString(CharArr str) {
    JSONUtil.writeString(str,out);
  }

  public void writeStringStart() {
    out.write('"');
  }

  public void writeStringChars(CharArr partialStr) {
    JSONUtil.writeStringPart(partialStr.getArray(), partialStr.getStart(), partialStr.getEnd(), out);
  }

  public void writeStringEnd() {
    out.write('"');
  }

  public void write(long number) {
    JSONUtil.writeNumber(number,out);
  }

  public void write(double number) {
    JSONUtil.writeNumber(number,out);
  }

  public void write(boolean bool) {
    JSONUtil.writeBoolean(bool,out);
  }

  public void writeNumber(CharArr digits) {
    out.write(digits);
  }

  public void writePartialNumber(CharArr digits) {
    out.write(digits);
  }

  public void startObject() {
    out.write('{');
    level++;
  }

  public void endObject() {
    out.write('}');
    level--;    
  }

  public void startArray() {
    out.write('[');
    level++;
  }

  public void endArray() {
    out.write(']');
    level--;
  }

  public void writeValueSeparator() {
    out.write(',');
  }

  public void writeNameSeparator() {
    out.write(':');    
  }

}

