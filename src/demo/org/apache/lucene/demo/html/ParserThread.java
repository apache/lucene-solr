package org.apache.lucene.demo.html;

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

import java.io.*;

class ParserThread extends Thread {
  HTMLParser parser;

  ParserThread(HTMLParser p) {
    parser = p;
  }

  @Override
  public void run() {				  // convert pipeOut to pipeIn
    try {
      try {					  // parse document to pipeOut
        parser.HTMLDocument();
      } catch (ParseException e) {
        System.out.println("Parse Aborted: " + e.getMessage());
      } catch (TokenMgrError e) {
        System.out.println("Parse Aborted: " + e.getMessage());
      } finally {
        parser.pipeOut.close();
        synchronized (parser) {
	      parser.summary.setLength(HTMLParser.SUMMARY_LENGTH);
	      parser.titleComplete = true;
	      parser.notifyAll();
	    }
      }
    } catch (IOException e) {
	  e.printStackTrace();
    }
  }
}
