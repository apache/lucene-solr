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
package org.apache.solr.benchmark.byTask.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

public class StreamEater extends Thread {
  
    private InputStream is;
    
    private int lines = 0;
    
    private OutputStream os;
    
    public StreamEater(InputStream is, OutputStream redirect) {
      this.is = is;
      this.os = redirect;
      setDaemon(true);
    }
    
    public void run() {
      PrintWriter pw = null;
      InputStreamReader isr = null;
      BufferedReader br = null;
      try {
        
        if (os != null) pw = new PrintWriter(os);
        
        isr = new InputStreamReader(is);
        br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
          if (pw != null) pw.println(lines + ":" + line);
          lines++;
        }
        if (pw != null) pw.flush();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      } finally {
        if (pw != null && os != System.out && os != System.err) {
          pw.close();
        }
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        if (isr != null) {
          try {
            isr.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        if (is != null) {
          try {
            is.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
      
    }
    
    public int getLines() {
      return lines;
    }
    
    /**
     * @param lines
     *          the lines to set
     */
    public void setLines(int lines) {
      this.lines = lines;
    }
  }
