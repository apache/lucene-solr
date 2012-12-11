package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.tasks.WriteLineDocTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.StreamUtils;

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
 * <li>line.parser - default to {@link HeaderLineParser} if a header line exists which differs 
 *     from {@link WriteLineDocTask#DEFAULT_FIELDS} and to {@link SimpleLineParser} otherwise.
 * </ul>
 */
public class LineDocSource extends ContentSource {

  /** Reader of a single input line into {@link DocData}. */
  public static abstract class LineParser {
    protected final String[] header;
    /** Construct with the header 
     * @param header header line found in the input file, or null if none
     */
    public LineParser(String[] header) {
      this.header = header; 
    }
    /** parse an input line and fill doc data appropriately */
    public abstract void parseLine(DocData docData, String line);
  }
  
  /** 
   * {@link LineParser} which ignores the header passed to its constructor
   * and assumes simply that field names and their order are the same 
   * as in {@link WriteLineDocTask#DEFAULT_FIELDS} 
   */
  public static class SimpleLineParser extends LineParser {
    public SimpleLineParser(String[] header) {
      super(header);
    }
    @Override
    public void parseLine(DocData docData, String line) {
      int k1 = 0;
      int k2 = line.indexOf(WriteLineDocTask.SEP, k1);
      if (k2<0) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format (missing: separator title::date)!");
      }
      docData.setTitle(line.substring(k1,k2));
      k1 = k2+1;
      k2 = line.indexOf(WriteLineDocTask.SEP, k1);
      if (k2<0) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format (missing: separator date::body)!");
      }
      docData.setDate(line.substring(k1,k2));
      k1 = k2+1;
      k2 = line.indexOf(WriteLineDocTask.SEP, k1);
      if (k2>=0) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format (too many separators)!");
      }
      // last one
      docData.setBody(line.substring(k1));
    }
  }
  
  /** 
   * {@link LineParser} which sets field names and order by 
   * the header - any header - of the lines file.
   * It is less efficient than {@link SimpleLineParser} but more powerful.
   */
  public static class HeaderLineParser extends LineParser {
    private enum FieldName { NAME , TITLE , DATE , BODY, PROP } 
    private final FieldName[] posToF;
    public HeaderLineParser(String[] header) {
      super(header);
      posToF = new FieldName[header.length];
      for (int i=0; i<header.length; i++) {
        String f = header[i];
        if (DocMaker.NAME_FIELD.equals(f)) {
          posToF[i] = FieldName.NAME;
        } else if (DocMaker.TITLE_FIELD.equals(f)) {
          posToF[i] = FieldName.TITLE;
        } else if (DocMaker.DATE_FIELD.equals(f)) {
          posToF[i] = FieldName.DATE;
        } else if (DocMaker.BODY_FIELD.equals(f)) {
          posToF[i] = FieldName.BODY;
        } else {
          posToF[i] = FieldName.PROP;
        }
      }
    }
    
    @Override
    public void parseLine(DocData docData, String line) {
      int n = 0;
      int k1 = 0;
      int k2;
      while ((k2 = line.indexOf(WriteLineDocTask.SEP, k1)) >= 0) {
        if (n>=header.length) {
          throw new RuntimeException("input line has invalid format: "+(n+1)+" fields instead of "+header.length+" :: [" + line + "]");
        }
        setDocDataField(docData, n, line.substring(k1,k2));
        ++n;
        k1 = k2 + 1;
      }
      if (n!=header.length-1) {
        throw new RuntimeException("input line has invalid format: "+(n+1)+" fields instead of "+header.length+" :: [" + line + "]");
      }
      // last one
      setDocDataField(docData, n, line.substring(k1)); 
    }

    private void setDocDataField(DocData docData, int position, String text) {
      switch(posToF[position]) {
        case NAME: 
          docData.setName(text);
          break;
        case TITLE: 
          docData.setTitle(text);
          break;
        case DATE: 
          docData.setDate(text);
          break;
        case BODY: 
          docData.setBody(text);
          break;
        case PROP:
          Properties p = docData.getProps();
          if (p==null) {
            p = new Properties();
            docData.setProps(p);
          }
          p.setProperty(header[position], text);
          break;
      }
    }
  }
  
  private File file;
  private BufferedReader reader;
  private int readCount;

  private LineParser docDataLineReader = null;
  private boolean skipHeaderLine = false;

  private synchronized void openFile() {
    try {
      if (reader != null) {
        reader.close();
      }
      InputStream is = StreamUtils.inputStream(file);
      reader = new BufferedReader(new InputStreamReader(is, encoding), StreamUtils.BUFFER_SIZE);
      if (skipHeaderLine) {
        reader.readLine(); // skip one line - the header line - already handled that info
      }
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
      if (line == null) {
        if (!forever) {
          throw new NoMoreDataException();
        }
        // Reset the file
        openFile();
        return getNextDocData(docData);
      }
      if (docDataLineReader == null) { // first line ever, one time initialization,
        docDataLineReader = createDocDataLineReader(line);
        if (skipHeaderLine) {
          return getNextDocData(docData);
        }
      }
      // increment IDS only once...
      myID = readCount++; 
    }
    
    // The date String was written in the format of DateTools.dateToString.
    docData.clear();
    docData.setID(myID);
    docDataLineReader.parseLine(docData, line);
    return docData;
  }

  private LineParser createDocDataLineReader(String line) {
    String[] header;
    String headIndicator = WriteLineDocTask.FIELDS_HEADER_INDICATOR + WriteLineDocTask.SEP;

    if (line.startsWith(headIndicator)) {
      header = line.substring(headIndicator.length()).split(Character.toString(WriteLineDocTask.SEP));
      skipHeaderLine = true; // mark to skip the header line when input file is reopened
    } else {
      header = WriteLineDocTask.DEFAULT_FIELDS;
    }
    
    // if a specific DocDataLineReader was configured, must respect it
    String docDataLineReaderClassName = getConfig().get("line.parser", null);
    if (docDataLineReaderClassName!=null) {
      try {
        final Class<? extends LineParser> clazz = 
          Class.forName(docDataLineReaderClassName).asSubclass(LineParser.class);
        Constructor<? extends LineParser> cnstr = clazz.getConstructor(new Class[]{String[].class});
        return cnstr.newInstance((Object)header);
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate "+docDataLineReaderClassName, e);
      }
    }

    // if this the simple case,   
    if (Arrays.deepEquals(header, WriteLineDocTask.DEFAULT_FIELDS)) {
      return new SimpleLineParser(header);
    }
    return new HeaderLineParser(header);
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
