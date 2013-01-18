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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.StreamUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.IOUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * A {@link ContentSource} which reads the English Wikipedia dump. You can read
 * the .bz2 file directly (it will be decompressed on the fly). Config
 * properties:
 * <ul>
 * <li>keep.image.only.docs=false|true (default <b>true</b>).
 * <li>docs.file=&lt;path to the file&gt;
 * </ul>
 */
public class EnwikiContentSource extends ContentSource {

  private class Parser extends DefaultHandler implements Runnable {
    private Thread t;
    private boolean threadDone;
    private boolean stopped = false;
    private String[] tuple;
    private NoMoreDataException nmde;
    private StringBuilder contents = new StringBuilder();
    private String title;
    private String body;
    private String time;
    private String id;
    
    String[] next() throws NoMoreDataException {
      if (t == null) {
        threadDone = false;
        t = new Thread(this);
        t.setDaemon(true);
        t.start();
      }
      String[] result;
      synchronized(this){
        while(tuple == null && nmde == null && !threadDone && !stopped) {
          try {
            wait();
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
        }
        if (tuple != null) {
          result = tuple;
          tuple = null;
          notify();
          return result;
        }
        if (nmde != null) {
          // Set to null so we will re-start thread in case
          // we are re-used:
          t = null;
          throw nmde;
        }
        // The thread has exited yet did not hit end of
        // data, so this means it hit an exception.  We
        // throw NoMorDataException here to force
        // benchmark to stop the current alg:
        throw new NoMoreDataException();
      }
    }
    
    String time(String original) {
      StringBuilder buffer = new StringBuilder();

      buffer.append(original.substring(8, 10));
      buffer.append('-');
      buffer.append(months[Integer.valueOf(original.substring(5, 7)).intValue() - 1]);
      buffer.append('-');
      buffer.append(original.substring(0, 4));
      buffer.append(' ');
      buffer.append(original.substring(11, 19));
      buffer.append(".000");

      return buffer.toString();
    }
    
    @Override
    public void characters(char[] ch, int start, int length) {
      contents.append(ch, start, length);
    }

    @Override
    public void endElement(String namespace, String simple, String qualified)
      throws SAXException {
      int elemType = getElementType(qualified);
      switch (elemType) {
        case PAGE:
          // the body must be null and we either are keeping image docs or the
          // title does not start with Image:
          if (body != null && (keepImages || !title.startsWith("Image:"))) {
            String[] tmpTuple = new String[LENGTH];
            tmpTuple[TITLE] = title.replace('\t', ' ');
            tmpTuple[DATE] = time.replace('\t', ' ');
            tmpTuple[BODY] = body.replaceAll("[\t\n]", " ");
            tmpTuple[ID] = id;
            synchronized(this) {
              while (tuple != null && !stopped) {
                try {
                  wait();
                } catch (InterruptedException ie) {
                  throw new ThreadInterruptedException(ie);
                }
              }
              tuple = tmpTuple;
              notify();
            }
          }
          break;
        case BODY:
          body = contents.toString();
          //workaround that startswith doesn't have an ignore case option, get at least 20 chars.
          String startsWith = body.substring(0, Math.min(10, contents.length())).toLowerCase(Locale.ROOT);
          if (startsWith.startsWith("#redirect")) {
            body = null;
          }
          break;
        case DATE:
          time = time(contents.toString());
          break;
        case TITLE:
          title = contents.toString();
          break;
        case ID:
          //the doc id is the first one in the page.  All other ids after that one can be ignored according to the schema
          if (id == null) {
            id = contents.toString();
          }
          break;
        default:
          // this element should be discarded.
      }
    }

    @Override
    public void run() {

      try {
        XMLReader reader = XMLReaderFactory.createXMLReader();
        reader.setContentHandler(this);
        reader.setErrorHandler(this);
        while(!stopped){
          final InputStream localFileIS = is;
          if (localFileIS != null) { // null means fileIS was closed on us 
            try {
              // To work around a bug in XERCES (XERCESJ-1257), we assume the XML is always UTF8, so we simply provide reader.
              CharsetDecoder decoder = IOUtils.CHARSET_UTF_8.newDecoder()
                  .onMalformedInput(CodingErrorAction.REPORT)
                  .onUnmappableCharacter(CodingErrorAction.REPORT);
              reader.parse(new InputSource(new BufferedReader(new InputStreamReader(localFileIS, decoder))));
            } catch (IOException ioe) {
              synchronized(EnwikiContentSource.this) {
                if (localFileIS != is) {
                  // fileIS was closed on us, so, just fall through
                } else
                  // Exception is real
                  throw ioe;
              }
            }
          }
          synchronized(this) {
            if (stopped || !forever) {
              nmde = new NoMoreDataException();
              notify();
              return;
            } else if (localFileIS == is) {
              // If file is not already re-opened then re-open it now
              is = openInputStream();
            }
          }
        }
      } catch (SAXException sae) {
        throw new RuntimeException(sae);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      } finally {
        synchronized(this) {
          threadDone = true;
          notify();
        }
      }
    }

    @Override
    public void startElement(String namespace, String simple, String qualified,
                             Attributes attributes) {
      int elemType = getElementType(qualified);
      switch (elemType) {
        case PAGE:
          title = null;
          body = null;
          time = null;
          id = null;
          break;
        // intentional fall-through.
        case BODY:
        case DATE:
        case TITLE:
        case ID:
          contents.setLength(0);
          break;
        default:
          // this element should be discarded.
      }
    }

    private void stop() {
      synchronized (this) {
        stopped = true;
        if (tuple != null) {
          tuple = null;
          notify();
        }
      }
    }

  }

  private static final Map<String,Integer> ELEMENTS = new HashMap<String,Integer>();
  private static final int TITLE = 0;
  private static final int DATE = TITLE + 1;
  private static final int BODY = DATE + 1;
  private static final int ID = BODY + 1;
  private static final int LENGTH = ID + 1;
  // LENGTH is used as the size of the tuple, so whatever constants we need that
  // should not be part of the tuple, we should define them after LENGTH.
  private static final int PAGE = LENGTH + 1;

  private static final String[] months = {"JAN", "FEB", "MAR", "APR",
                                  "MAY", "JUN", "JUL", "AUG",
                                  "SEP", "OCT", "NOV", "DEC"};

  static {
    ELEMENTS.put("page", Integer.valueOf(PAGE));
    ELEMENTS.put("text", Integer.valueOf(BODY));
    ELEMENTS.put("timestamp", Integer.valueOf(DATE));
    ELEMENTS.put("title", Integer.valueOf(TITLE));
    ELEMENTS.put("id", Integer.valueOf(ID));
  }
  
  /**
   * Returns the type of the element if defined, otherwise returns -1. This
   * method is useful in startElement and endElement, by not needing to compare
   * the element qualified name over and over.
   */
  private final static int getElementType(String elem) {
    Integer val = ELEMENTS.get(elem);
    return val == null ? -1 : val.intValue();
  }
  
  private File file;
  private boolean keepImages = true;
  private InputStream is;
  private Parser parser = new Parser();
  
  @Override
  public void close() throws IOException {
    synchronized (EnwikiContentSource.this) {
      parser.stop();
      if (is != null) {
        is.close();
        is = null;
      }
    }
  }
  
  @Override
  public synchronized DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    String[] tuple = parser.next();
    docData.clear();
    docData.setName(tuple[ID]);
    docData.setBody(tuple[BODY]);
    docData.setDate(tuple[DATE]);
    docData.setTitle(tuple[TITLE]);
    return docData;
  }

  @Override
  public void resetInputs() throws IOException {
    super.resetInputs();
    is = openInputStream();
  }

  /** Open the input stream. */
  protected InputStream openInputStream() throws IOException {
    return StreamUtils.inputStream(file);
  }
  
  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    keepImages = config.get("keep.image.only.docs", true);
    String fileName = config.get("docs.file", null);
    if (fileName != null) {
      file = new File(fileName).getAbsoluteFile();
    }
  }
  
}
