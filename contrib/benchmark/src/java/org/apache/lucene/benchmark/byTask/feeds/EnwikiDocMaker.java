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

import org.xml.sax.XMLReader;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.IOException;
import java.io.FileInputStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.benchmark.byTask.utils.Config;

/**
 * A LineDocMaker which reads the uncompressed english wikipedia dump.
 *
 * Config properties:
 * keep.image.only.docs=false|true
 * <br/>
 * Plus those available in LineDocMaker
 *
 *
 * @see org.apache.lucene.benchmark.byTask.feeds.LineDocMaker
 */
public class EnwikiDocMaker extends LineDocMaker {
  protected boolean keepImages = true;
  static final int TITLE = 0;
  static final int DATE = TITLE+1;
  static final int BODY = DATE+1;
  static final int ID = BODY + 1;
  static final int LENGTH = ID+1;

  static final String[] months = {"JAN", "FEB", "MAR", "APR",
                                  "MAY", "JUN", "JUL", "AUG",
                                  "SEP", "OCT", "NOV", "DEC"};

  public void setConfig(Config config) {
    super.setConfig(config);
    keepImages = config.get("keep.image.only.docs", true);
  }

  class Parser extends DefaultHandler implements Runnable {

    Thread t;
    boolean threadDone;

    public void run() {

      try {
        XMLReader reader =
          XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
        reader.setContentHandler(this);
        reader.setErrorHandler(this);
        while(true){
          final FileInputStream localFileIS = fileIS;
          try {
            InputSource is = new InputSource(localFileIS);
            reader.parse(is);
          } catch (IOException ioe) {
            synchronized(EnwikiDocMaker.this) {
              if (localFileIS != fileIS) {
                // fileIS was closed on us, so, just fall
                // through
              } else
                // Exception is real
                throw ioe;
            }
          }
          synchronized(this) {
            if (!forever) {
              nmde = new NoMoreDataException();
              notify();
              return;
            } else if (localFileIS == fileIS) {
              // If file is not already re-opened then
              // re-open it now
              openFile();
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

    String[] tuple;
    NoMoreDataException nmde;

    String[] next() throws NoMoreDataException {
      if (t == null) {
        threadDone = false;
        t = new Thread(this);
        t.setDaemon(true);
        t.start();
      }
      String[] result;
      synchronized(this){
        while(tuple == null && nmde == null && !threadDone) {
          try {
            wait();
          } catch (InterruptedException ie) {
          }
        }
        if (nmde != null) {
          // Set to null so we will re-start thread in case
          // we are re-used:
          t = null;
          throw nmde;
        }
        if (t != null && threadDone)
          // The thread has exited yet did not hit end of
          // data, so this means it hit an exception.  We
          // throw NoMorDataException here to force
          // benchmark to stop the current alg:
          throw new NoMoreDataException();
        result = tuple;
        tuple = null;
        notify();
      }
      return result;
    }

    StringBuffer contents = new StringBuffer();

    public void characters(char[] ch, int start, int length) {
      contents.append(ch, start, length);
    }

    String title;
    String body;
    String time;
    String id;


    
    public void startElement(String namespace,
                             String simple,
                             String qualified,
                             Attributes attributes) {
      if (qualified.equals("page")) {
        title = null;
        body = null;
        time = null;
        id = null;
      } else if (qualified.equals("text")) {
        contents.setLength(0);
      } else if (qualified.equals("timestamp")) {
        contents.setLength(0);
      } else if (qualified.equals("title")) {
        contents.setLength(0);
      } else if (qualified.equals("id")) {
        contents.setLength(0);
      }
    }

    String time(String original) {
      StringBuffer buffer = new StringBuffer();

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

    public void create(String title, String time, String body, String id) {
      String[] t = new String[LENGTH];
      t[TITLE] = title.replace('\t', ' ');
      t[DATE] = time.replace('\t', ' ');
      t[BODY] = body.replaceAll("[\t\n]", " ");
      t[ID] = id;
      synchronized(this) {
        while(tuple!=null) {
          try {
            wait();
          } catch (InterruptedException ie) {
          }
        }
        tuple = t;
        notify();
      }
    }

    public void endElement(String namespace, String simple, String qualified)
      throws SAXException {
      if (qualified.equals("title")) {
        title = contents.toString();
      } else if (qualified.equals("text")) {
        body = contents.toString();
        //workaround that startswith doesn't have an ignore case option, get at least 20 chars.
        String startsWith = body.substring(0, Math.min(10, contents.length())).toLowerCase();
        if (startsWith.startsWith("#redirect")) {
          body = null;
        }
      } else if (qualified.equals("timestamp")) {
        time = time(contents.toString());
      } else if (qualified.equals("id") && id == null) {//just get the first id
        id = contents.toString();
      }
      else if (qualified.equals("page")) {
        //the body must be null and we either are keeping image docs or the title does not start with Image:
        if (body != null && (keepImages == true || title.startsWith("Image:") == false)) {
          create(title, time, body, id);
        }
      }
    }
  }

  Parser parser = new Parser();

  class DocState extends LineDocMaker.DocState {
    public Document setFields(String[] tuple) {
      titleField.setValue(tuple[TITLE]);
      dateField.setValue(tuple[DATE]);
      bodyField.setValue(tuple[BODY]);
      idField.setValue(tuple[ID]);
      return doc;
    }
  }

  private DocState getDocState() {
    DocState ds = (DocState) docState.get();
    if (ds == null) {
      ds = new DocState();
      docState.set(ds);
    }
    return ds;
  }

  public Document makeDocument() throws Exception {
    String[] tuple = parser.next();
    return getDocState().setFields(tuple);
  }

}