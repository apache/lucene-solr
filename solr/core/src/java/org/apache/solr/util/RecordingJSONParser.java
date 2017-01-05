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
package org.apache.solr.util;

import java.io.IOException;
import java.io.Reader;

import org.noggit.JSONParser;

public class RecordingJSONParser extends JSONParser {
  static ThreadLocal<char[]> buf = new ThreadLocal<>();
  private final char[] bufCopy;
  //global position is the global position at the beginning of my buffer
  private long globalPosition = 0;

  private StringBuilder sb = new StringBuilder();
  private boolean objectStarted = false;
  private long lastMarkedPosition = 0;
  private long lastGlobalPosition = 0;
  private static final int BUFFER_SIZE = 8192;


  public RecordingJSONParser(Reader in) {
    super(in, getChars());
    bufCopy = buf.get();
    buf.remove();
  }

  static char[] getChars() {
    buf.set(new char[BUFFER_SIZE]);
    return buf.get();
  }

  private void recordChar(int aChar) {
    if (objectStarted) {
      sb.append((char) aChar);
    } else if (aChar == '{') {
      sb.append((char) aChar);
      objectStarted = true;
    }
  }

  public void resetBuf() {
    sb = new StringBuilder();
    objectStarted = false;
  }

  @Override
  public int nextEvent() throws IOException {
    captureMissing();
    return super.nextEvent();
  }

  private void captureMissing() {
    long currPosition = getPosition() - globalPosition;
    if(currPosition < 0){
      System.out.println("ERROR");
    }

    if (currPosition > lastMarkedPosition) {
      for (long i = lastMarkedPosition; i < currPosition; i++) {
        recordChar(bufCopy[(int) i]);
      }
    } else if (currPosition < lastMarkedPosition) {
      for (long i = 0; i < currPosition; i++) {
        recordChar(bufCopy[(int) i]);
      }
    } else if (currPosition == BUFFER_SIZE && lastGlobalPosition != globalPosition) {
      for (long i = 0; i < currPosition; i++) {
        recordChar(bufCopy[(int) i]);
      }
    }

    lastGlobalPosition = globalPosition;
    lastMarkedPosition = currPosition;
  }


  @Override
  protected void fill() throws IOException {
    captureMissing();
    super.fill();
    this.globalPosition = getPosition();

  }

  public String getBuf() {
    captureMissing();
    if (sb != null) return sb.toString();
    return null;
  }

  public JSONParser.ParseException error(String msg) {
    return err(msg);
  }
}
