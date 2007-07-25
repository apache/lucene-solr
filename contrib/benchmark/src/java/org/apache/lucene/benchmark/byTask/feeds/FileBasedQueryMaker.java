package org.apache.lucene.benchmark.byTask.feeds;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright 2004 The Apache Software Foundation
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Create queries from a FileReader.  One per line, pass them through the
 * QueryParser.  Lines beginning with # are treated as comments
 *
 * File can be specified as a absolute, relative or resource.
 * Two properties can be set:
 * file.query.maker.file=&lt;Full path to file containing queries&gt;
 * <br/>
 * file.query.maker.default.field=&lt;Name of default field - Default value is "body"&gt;
 *
 * Example:
 * file.query.maker.file=c:/myqueries.txt
 * file.query.maker.default.field=body
 */
public class FileBasedQueryMaker extends AbstractQueryMaker implements QueryMaker{


  protected Query[] prepareQueries() throws Exception {

    Analyzer anlzr = (Analyzer) Class.forName(config.get("analyzer",
            "org.apache.lucene.analysis.standard.StandardAnalyzer")).newInstance();
    String defaultField = config.get("file.query.maker.default.field", BasicDocMaker.BODY_FIELD);
    QueryParser qp = new QueryParser(defaultField, anlzr);

    List qq = new ArrayList();
    String fileName = config.get("file.query.maker.file", null);
    if (fileName != null)
    {
      File file = new File(fileName);
      Reader reader = null;
      if (file != null && file.exists())
      {
        reader = new FileReader(file);
      } else {
        //see if we can find it as a resource
        InputStream asStream = FileBasedQueryMaker.class.getClassLoader().getResourceAsStream(fileName);
        if (asStream != null) {
          reader = new InputStreamReader(asStream);
        }
      }
      if (reader != null) {
        BufferedReader buffered = new BufferedReader(reader);
        String line = null;
        int lineNum = 0;
        while ((line = buffered.readLine()) != null)
        {
          line = line.trim();
          if (!line.equals("") && !line.startsWith("#"))
          {
            Query query = null;
            try {
              query = qp.parse(line);
            } catch (ParseException e) {
              System.err.println("Exception: " + e.getMessage() + " occurred while parsing line: " + lineNum + " Text: " + line);
            }
            qq.add(query);
          }
          lineNum++;
        }
      } else {
        System.err.println("No Reader available for: " + fileName);
      }
    }
    Query [] result = (Query[]) qq.toArray(new Query[qq.size()]) ;
    return result;
  }
}
