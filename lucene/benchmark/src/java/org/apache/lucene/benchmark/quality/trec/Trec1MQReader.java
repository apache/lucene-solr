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
 * Read topics of TREC 1MQ track.
 * <p>
 * Expects this topic format -
 * <pre>
 *   qnum:qtext
 * </pre>
 * Comment lines starting with '#' are ignored.
 * <p>
 * All topics will have a single name value pair.
 */
public class Trec1MQReader {

  private String name;
  
  /**
   *  Constructor for Trec's 1MQ TopicsReader
   *  @param name name of name-value pair to set for all queries.
   */
  public Trec1MQReader(String name) {
    super();
    this.name = name;
  }

  /**
   * Read quality queries from trec 1MQ format topics file.
   * @param reader where queries are read from.
   * @return the result quality queries.
   * @throws IOException if cannot read the queries.
   */
  public QualityQuery[] readQueries(BufferedReader reader) throws IOException {
    ArrayList<QualityQuery> res = new ArrayList<>();
    String line;
    try {
      while (null!=(line=reader.readLine())) {
        line = line.trim();
        if (line.startsWith("#")) {
          continue;
        }
        // id
        int k = line.indexOf(":");
        String id = line.substring(0,k).trim();
        // qtext
        String qtext = line.substring(k+1).trim();
        // we got a topic!
        HashMap<String,String> fields = new HashMap<>();
        fields.put(name,qtext);
        //System.out.println("id: "+id+" qtext: "+qtext+"  line: "+line);
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

}
