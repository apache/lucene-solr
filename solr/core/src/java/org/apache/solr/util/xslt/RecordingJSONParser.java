package org.apache.solr.util.xslt;

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


import java.io.IOException;
import java.io.Reader;

import org.noggit.CharArr;
import org.noggit.JSONParser;

public class RecordingJSONParser extends JSONParser{
  public RecordingJSONParser(Reader in) { super(in); }

  private StringBuilder sb = new StringBuilder() ;
  private long position;
  private boolean objectStarted =false;



  @Override
  protected int getChar() throws IOException {
    int aChar = super.getChar();
    if(aChar == '{') objectStarted =true;
    if(getPosition() >position) recordChar((char) aChar); // check before adding if a pushback happened ignore
    position= getPosition();
    return aChar;
  }

  private void recordChar(int aChar) {
    if(objectStarted)
      sb.append((char) aChar);
  }
  private void recordStr(String s) {
    if(objectStarted) sb.append(s);
  }

  @Override
  public CharArr getStringChars() throws IOException {
    CharArr chars = super.getStringChars();
    recordStr(chars.toString());
    position = getPosition();
    // if reading a String , the getStringChars do not return the closing single quote or double quote
    //so, try to capture that
    if(chars.getArray().length >=chars.getStart()+chars.size()) {
      char next = chars.getArray()[chars.getStart() + chars.size()];
      if(next =='"' || next == '\'') {
        recordChar(next);
      }
    }
    return chars;
  }

  public void resetBuf(){
    sb = new StringBuilder();
    objectStarted=false;
  }


  public String getBuf() {
    if(sb != null) return sb.toString();
    return null;
  }
}
