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
package org.apache.lucene.benchmark.quality.trec;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.lucene.benchmark.quality.QualityQuery;

/**
 * Read TREC topics.
 * <p>
 * Expects this topic format -
 * <pre>
 *   &lt;top&gt;
 *   &lt;num&gt; Number: nnn
 *     
 *   &lt;title&gt; title of the topic
 *     
 *   &lt;desc&gt; Description:
 *   description of the topic
 *     
 *   &lt;narr&gt; Narrative:
 *   "story" composed by assessors.
 *    
 *   &lt;/top&gt;
 * </pre>
 * Comment lines starting with '#' are ignored.
 */
public class TrecTopicsReader {

  private static final String newline = System.getProperty("line.separator");
  
  /**
   *  Constructor for Trec's TopicsReader
   */
  public TrecTopicsReader() {
    super();
  }

  /**
   * Read quality queries from trec format topics file.
   * @param reader where queries are read from.
   * @return the result quality queries.
   * @throws IOException if cannot read the queries.
   */
  public QualityQuery[] readQueries(BufferedReader reader) throws IOException {
    ArrayList<QualityQuery> res = new ArrayList<>();
    StringBuilder sb;
    try {
      while (null!=(sb=read(reader,"<top>",null,false,false))) {
        HashMap<String,String> fields = new HashMap<>();
        // id
        sb = read(reader,"<num>",null,true,false);
        int k = sb.indexOf(":");
        String id = sb.substring(k+1).trim();
        // title
        sb = read(reader,"<title>",null,true,false);
        k = sb.indexOf(">");
        String title = sb.substring(k+1).trim();
        // description
        read(reader,"<desc>",null,false,false);
        sb.setLength(0);
        String line = null;
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("<narr>"))
            break;
          if (sb.length() > 0) sb.append(' ');
          sb.append(line);
        }
        String description = sb.toString().trim();
        // narrative
        sb.setLength(0);
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("</top>"))
            break;
          if (sb.length() > 0) sb.append(' ');
          sb.append(line);
        }
        String narrative = sb.toString().trim();
        // we got a topic!
        fields.put("title",title);
        fields.put("description",description);
        fields.put("narrative", narrative);
        QualityQuery topic = new QualityQuery(id,fields);
        res.add(topic);
      }
    } finally {
      reader.close();
    }
    // sort result array (by ID) 
    QualityQuery qq[] = res.toArray(new QualityQuery[0]);
    Arrays.sort(qq);
    return qq;
  }

  // read until finding a line that starts with the specified prefix
  private StringBuilder read (BufferedReader reader, String prefix, StringBuilder sb, boolean collectMatchLine, boolean collectAll) throws IOException {
    sb = (sb==null ? new StringBuilder() : sb);
    String sep = "";
    while (true) {
      String line = reader.readLine();
      if (line==null) {
        return null;
      }
      if (line.startsWith(prefix)) {
        if (collectMatchLine) {
          sb.append(sep+line);
          sep = newline;
        }
        break;
      }
      if (collectAll) {
        sb.append(sep+line);
        sep = newline;
      }
    }
    //System.out.println("read: "+sb);
    return sb;
  }
}
