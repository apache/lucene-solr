package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.lucene.benchmark.byTask.tasks.WriteLineDocTask;
import org.apache.lucene.benchmark.byTask.utils.Config;

/**
 * A {@link ContentSource} reading one line at a time as a
 * {@link org.apache.lucene.document.Document} from a single file. This saves IO
 * cost (over DirContentSource) of recursing through a directory and opening a
 * new file for every document.<br>
 * The expected format of each line is (arguments are separated by &lt;TAB&gt;):
 * <i>title, date, body</i>. If a line is read in a different format, a
 * {@link RuntimeException} will be thrown. In general, you should use this
 * content source for files that were created with {@link WriteLineDocTask}.<br>
 * <br>
 * Config properties:
 * <ul>
 * <li>docs.file=&lt;path to the file&gt;
 * <li>content.source.encoding - default to UTF-8.
 * </ul>
 */
public class LineDocSource extends ContentSource {

  private final static char SEP = WriteLineDocTask.SEP;

  private File file;
  private BufferedReader reader;
  private int readCount;

  private synchronized void openFile() {
    try {
      if (reader != null) {
        reader.close();
      }
      InputStream is = getInputStream(file);
      reader = new BufferedReader(new InputStreamReader(is, encoding), BUFFER_SIZE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }
  
  @Override
  public DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    final String line;
    final int myID;
    
    synchronized(this) {
      line = reader.readLine();
      myID = readCount++;
      if (line == null) {
        if (!forever) {
          throw new NoMoreDataException();
        }
        // Reset the file
        openFile();
        return getNextDocData(docData);
      }
    }
    
    // A line must be in the following format. If it's not, fail !
    // title <TAB> date <TAB> body <NEWLINE>
    int spot = line.indexOf(SEP);
    if (spot == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }
    int spot2 = line.indexOf(SEP, 1 + spot);
    if (spot2 == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }
    // The date String was written in the format of DateTools.dateToString.
    docData.clear();
    docData.setID(myID);
    docData.setBody(line.substring(1 + spot2, line.length()));
    docData.setTitle(line.substring(0, spot));
    docData.setDate(line.substring(1 + spot, spot2));
    return docData;
  }

  @Override
  public void resetInputs() throws IOException {
    super.resetInputs();
    openFile();
  }
  
  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    String fileName = config.get("docs.file", null);
    if (fileName == null) {
      throw new IllegalArgumentException("docs.file must be set");
    }
    file = new File(fileName).getAbsoluteFile();
    if (encoding == null) {
      encoding = "UTF-8";
    }
  }

}
