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
package org.apache.solr.handler.dataimport;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;


/**
 * <p>
 * An {@link EntityProcessor} instance which can stream lines of text read from a 
 * datasource. Options allow lines to be explicitly skipped or included in the index.
 * </p>
 * <p>
 * Attribute summary 
 * <ul>
 * <li>url is the required location of the input file. If this value is
 *     relative, it assumed to be relative to baseLoc.</li>
 * <li>acceptLineRegex is an optional attribute that if present discards any 
 *     line which does not match the regExp.</li>
 * <li>skipLineRegex is an optional attribute that is applied after any 
 *     acceptLineRegex and discards any line which matches this regExp.</li>
 * </ul>
 * <p>
 * Although envisioned for reading lines from a file or url, LineEntityProcessor may also be useful
 * for dealing with change lists, where each line contains filenames which can be used by subsequent entities
 * to parse content from those files.
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.4
 * @see Pattern
 */
public class LineEntityProcessor extends EntityProcessorBase {
  private Pattern acceptLineRegex, skipLineRegex;
  private String url;
  private BufferedReader reader;

  /**
   * Parses each of the entity attributes.
   */
  @Override
  public void init(Context context) {
    super.init(context);
    String s;

    // init a regex to locate files from the input we want to index
    s = context.getResolvedEntityAttribute(ACCEPT_LINE_REGEX);
    if (s != null) {
      acceptLineRegex = Pattern.compile(s);
    }

    // init a regex to locate files from the input to be skipped
    s = context.getResolvedEntityAttribute(SKIP_LINE_REGEX);
    if (s != null) {
      skipLineRegex = Pattern.compile(s);
    }

    // the FileName is required.
    url = context.getResolvedEntityAttribute(URL);
    if (url == null) throw
      new DataImportHandlerException(DataImportHandlerException.SEVERE,
           "'"+ URL +"' is a required attribute");
  }


  /**
   * Reads lines from the url till it finds a lines that matches the
   * optional acceptLineRegex and does not match the optional skipLineRegex.
   *
   * @return A row containing a minimum of one field "rawLine" or null to signal
   * end of file. The rawLine is the as line as returned by readLine()
   * from the url. However transformers can be used to create as 
   * many other fields as required.
   */
  @Override
  public Map<String, Object> nextRow() {
    if (reader == null) {
      reader = new BufferedReader((Reader) context.getDataSource().getData(url));
    }

    String line;
    
    while ( true ) { 
      // read a line from the input file
      try {
        line = reader.readLine();
      }
      catch (IOException exp) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
             "Problem reading from input", exp);
      }
  
      // end of input
      if (line == null) {
        closeResources();
        return null;
      }

      // First scan whole line to see if we want it
      if (acceptLineRegex != null && ! acceptLineRegex.matcher(line).find()) continue;
      if (skipLineRegex != null &&   skipLineRegex.matcher(line).find()) continue;
      // Contruct the 'row' of fields
      Map<String, Object> row = new HashMap<>();
      row.put("rawLine", line);
      return row;
    }
  }
  
  public void closeResources() {
    if (reader != null) {
      IOUtils.closeQuietly(reader);
    }
    reader= null;
  }

    @Override
    public void destroy() {
      closeResources();
      super.destroy();
    }

  /**
   * Holds the name of entity attribute that will be parsed to obtain
   * the filename containing the changelist.
   */
  public static final String URL = "url";

  /**
   * Holds the name of entity attribute that will be parsed to obtain
   * the pattern to be used when checking to see if a line should
   * be returned.
   */
  public static final String ACCEPT_LINE_REGEX = "acceptLineRegex";

  /**
   * Holds the name of entity attribute that will be parsed to obtain
   * the pattern to be used when checking to see if a line should
   * be ignored.
   */
  public static final String SKIP_LINE_REGEX = "skipLineRegex";
}
