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
package org.apache.lucene.search.suggest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;


/**
 * Dictionary represented by a text file.
 * 
 * <p>Format allowed: 1 entry per line:<br>
 * An entry can be: <br>
 * <ul>
 * <li>suggestion</li>
 * <li>suggestion <code>fieldDelimiter</code> weight</li>
 * <li>suggestion <code>fieldDelimiter</code> weight <code>fieldDelimiter</code> payload</li>
 * </ul>
 * where the default <code>fieldDelimiter</code> is {@value #DEFAULT_FIELD_DELIMITER}<br>
 * <p>
 * <b>NOTE:</b> 
 * <ul>
 * <li>In order to have payload enabled, the first entry has to have a payload</li>
 * <li>If the weight for an entry is not specified then a value of 1 is used</li>
 * <li>A payload cannot be specified without having the weight specified for an entry</li>
 * <li>If the payload for an entry is not specified (assuming payload is enabled) 
 *  then an empty payload is returned</li>
 * <li>An entry cannot have more than two <code>fieldDelimiter</code></li>
 * </ul>
 * <p>
 * <b>Example:</b><br>
 * word1 word2 TAB 100 TAB payload1<br>
 * word3 TAB 101<br>
 * word4 word3 TAB 102<br>
 */
public class FileDictionary implements Dictionary {

  /**
   * Tab-delimited fields are most common thus the default, but one can override this via the constructor
   */
  public final static String DEFAULT_FIELD_DELIMITER = "\t";
  private BufferedReader in;
  private String line;
  private boolean done = false;
  private final String fieldDelimiter;

  /**
   * Creates a dictionary based on an inputstream.
   * Using {@link #DEFAULT_FIELD_DELIMITER} as the 
   * field seperator in a line.
   * <p>
   * NOTE: content is treated as UTF-8
   */
  public FileDictionary(InputStream dictFile) {
    this(dictFile, DEFAULT_FIELD_DELIMITER);
  }

  /**
   * Creates a dictionary based on a reader.
   * Using {@link #DEFAULT_FIELD_DELIMITER} as the 
   * field seperator in a line.
   */
  public FileDictionary(Reader reader) {
    this(reader, DEFAULT_FIELD_DELIMITER);
  }
  
  /**
   * Creates a dictionary based on a reader. 
   * Using <code>fieldDelimiter</code> to seperate out the
   * fields in a line.
   */
  public FileDictionary(Reader reader, String fieldDelimiter) {
    in = new BufferedReader(reader);
    this.fieldDelimiter = fieldDelimiter;
  }
  
  /**
   * Creates a dictionary based on an inputstream.
   * Using <code>fieldDelimiter</code> to seperate out the
   * fields in a line.
   * <p>
   * NOTE: content is treated as UTF-8
   */
  public FileDictionary(InputStream dictFile, String fieldDelimiter) {
    in = new BufferedReader(IOUtils.getDecodingReader(dictFile, StandardCharsets.UTF_8));
    this.fieldDelimiter = fieldDelimiter;
  }

  @Override
  public InputIterator getEntryIterator() {
    try {
      return new FileIterator();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  final class FileIterator implements InputIterator {
    private long curWeight;
    private final BytesRefBuilder spare = new BytesRefBuilder();
    private BytesRefBuilder curPayload = new BytesRefBuilder();
    private boolean isFirstLine = true;
    private boolean hasPayloads = false;
    
    private FileIterator() throws IOException {
      line = in.readLine();
      if (line == null) {
        done = true;
        IOUtils.close(in);
      } else {
        String[] fields = line.split(fieldDelimiter);
        if (fields.length > 3) {
          throw new IllegalArgumentException("More than 3 fields in one line");
        } else if (fields.length == 3) { // term, weight, payload
          hasPayloads = true;
          spare.copyChars(fields[0]);
          readWeight(fields[1]);
          curPayload.copyChars(fields[2]);
        } else if (fields.length == 2) { // term, weight
          spare.copyChars(fields[0]);
          readWeight(fields[1]);
        } else { // only term
          spare.copyChars(fields[0]);
          curWeight = 1;
        }
      }
    }
    
    @Override
    public long weight() {
      return curWeight;
    }

    @Override
    public BytesRef next() throws IOException {
      if (done) {
        return null;
      }
      if (isFirstLine) {
        isFirstLine = false;
        return spare.get();
      }
      line = in.readLine();
      if (line != null) {
        String[] fields = line.split(fieldDelimiter);
        if (fields.length > 3) {
          throw new IllegalArgumentException("More than 3 fields in one line");
        } else if (fields.length == 3) { // term, weight and payload
          spare.copyChars(fields[0]);
          readWeight(fields[1]);
          if (hasPayloads) {
            curPayload.copyChars(fields[2]);
          }
        } else if (fields.length == 2) { // term, weight
          spare.copyChars(fields[0]);
          readWeight(fields[1]);
          if (hasPayloads) { // have an empty payload
            curPayload = new BytesRefBuilder();
          }
        } else { // only term
          spare.copyChars(fields[0]);
          curWeight = 1;
          if (hasPayloads) {
            curPayload = new BytesRefBuilder();
          }
        }
        return spare.get();
      } else {
        done = true;
        IOUtils.close(in);
        return null;
      }
    }

    @Override
    public BytesRef payload() {
      return (hasPayloads) ? curPayload.get() : null;
    }

    @Override
    public boolean hasPayloads() {
      return hasPayloads;
    }
    
    private void readWeight(String weight) {
      // keep reading floats for bw compat
      try {
        curWeight = Long.parseLong(weight);
      } catch (NumberFormatException e) {
        curWeight = (long)Double.parseDouble(weight);
      }
    }

    @Override
    public Set<BytesRef> contexts() {
      return null;
    }

    @Override
    public boolean hasContexts() {
      return false;
    }
  }
}
