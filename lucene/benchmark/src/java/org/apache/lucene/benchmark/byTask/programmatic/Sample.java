package org.apache.lucene.benchmark.byTask.programmatic;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.AddDocTask;
import org.apache.lucene.benchmark.byTask.tasks.CloseIndexTask;
import org.apache.lucene.benchmark.byTask.tasks.CreateIndexTask;
import org.apache.lucene.benchmark.byTask.tasks.RepSumByNameTask;
import org.apache.lucene.benchmark.byTask.tasks.TaskSequence;
import org.apache.lucene.benchmark.byTask.utils.Config;

/**
 * Sample performance test written programmatically - no algorithm file is needed here.
 */
public class Sample {

  public static void main(String[] args) throws Exception {
    Properties p = initProps();
    Config conf = new Config(p);
    PerfRunData runData = new PerfRunData(conf);
    
    // 1. top sequence
    TaskSequence top = new TaskSequence(runData,null,null,false); // top level, not parallel
    
    // 2. task to create the index
    CreateIndexTask create = new CreateIndexTask(runData);
    top.addTask(create);
    
    // 3. task seq to add 500 docs (order matters - top to bottom - add seq to top, only then add to seq)
    TaskSequence seq1 = new TaskSequence(runData,"AddDocs",top,false);
    seq1.setRepetitions(500);
    seq1.setNoChildReport();
    top.addTask(seq1);

    // 4. task to add the doc
    AddDocTask addDoc = new AddDocTask(runData);
    //addDoc.setParams("1200"); // doc size limit if supported
    seq1.addTask(addDoc); // order matters 9see comment above)

    // 5. task to close the index
    CloseIndexTask close = new CloseIndexTask(runData);
    top.addTask(close);

    // task to report
    RepSumByNameTask rep = new RepSumByNameTask(runData);
    top.addTask(rep);

    // print algorithm
    System.out.println(top.toString());
    
    // execute
    top.doLogic();
  }

  // Sample programmatic settings. Could also read from file.
  private static Properties initProps() {
    Properties p = new Properties();
    p.setProperty ( "task.max.depth.log"  , "3" );
    p.setProperty ( "max.buffered"        , "buf:10:10:100:100:10:10:100:100" );
    p.setProperty ( "doc.maker"           , "org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource" );
    p.setProperty ( "log.step"            , "2000" );
    p.setProperty ( "doc.delete.step"     , "8" );
    p.setProperty ( "analyzer"            , "org.apache.lucene.analysis.standard.StandardAnalyzer" );
    p.setProperty ( "doc.term.vector"     , "false" );
    p.setProperty ( "directory"           , "FSDirectory" );
    p.setProperty ( "query.maker"         , "org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker" );
    p.setProperty ( "doc.stored"          , "true" );
    p.setProperty ( "docs.dir"            , "reuters-out" );
    p.setProperty ( "compound"            , "cmpnd:true:true:true:true:false:false:false:false" );
    p.setProperty ( "doc.tokenized"       , "true" );
    p.setProperty ( "merge.factor"        , "mrg:10:100:10:100:10:100:10:100" );
    return p;
  }
  
  

}
