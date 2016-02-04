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
package org.apache.lucene.benchmark.byTask.feeds;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.benchmark.byTask.tasks.NewAnalyzerTask;
import org.apache.lucene.util.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Create queries from a FileReader.  One per line, pass them through the
 * QueryParser.  Lines beginning with # are treated as comments
 *
 * File can be specified as a absolute, relative or resource.
 * Two properties can be set:
 * file.query.maker.file=&lt;Full path to file containing queries&gt;
 * <br>
 * file.query.maker.default.field=&lt;Name of default field - Default value is "body"&gt;
 *
 * Example:
 * file.query.maker.file=c:/myqueries.txt
 * file.query.maker.default.field=body
 */
public class FileBasedQueryMaker extends AbstractQueryMaker implements QueryMaker{


  @Override
  protected Query[] prepareQueries() throws Exception {

    Analyzer anlzr = NewAnalyzerTask.createAnalyzer(config.get("analyzer",
            "org.apache.lucene.analysis.standard.StandardAnalyzer"));
    String defaultField = config.get("file.query.maker.default.field", DocMaker.BODY_FIELD);
    QueryParser qp = new QueryParser(defaultField, anlzr);
    qp.setAllowLeadingWildcard(true);

    List<Query> qq = new ArrayList<>();
    String fileName = config.get("file.query.maker.file", null);
    if (fileName != null)
    {
      Path path = Paths.get(fileName);
      Reader reader = null;
      // note: we use a decoding reader, so if your queries are screwed up you know
      if (Files.exists(path)) {
        reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
      } else {
        //see if we can find it as a resource
        InputStream asStream = FileBasedQueryMaker.class.getClassLoader().getResourceAsStream(fileName);
        if (asStream != null) {
          reader = IOUtils.getDecodingReader(asStream, StandardCharsets.UTF_8);
        }
      }
      if (reader != null) {
        try {
          BufferedReader buffered = new BufferedReader(reader);
          String line = null;
          int lineNum = 0;
          while ((line = buffered.readLine()) != null) {
            line = line.trim();
            if (line.length() != 0 && !line.startsWith("#")) {
              try {
                qq.add(qp.parse(line));
              } catch (ParseException e) {
                System.err.println("Exception: " + e.getMessage() + " occurred while parsing line: " + lineNum + " Text: " + line);
              }
            }
            lineNum++;
          }
        } finally {
          reader.close();
        }
      } else {
        System.err.println("No Reader available for: " + fileName);
      }
      
    }
    return qq.toArray(new Query[qq.size()]) ;
  }
}
