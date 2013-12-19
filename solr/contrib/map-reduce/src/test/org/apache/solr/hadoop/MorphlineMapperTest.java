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
package org.apache.solr.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.lucene.util.Constants;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.morphline.MorphlineMapper;
import org.junit.BeforeClass;
import org.junit.Test;

public class MorphlineMapperTest extends MRUnitBase {
  
  @BeforeClass
  public static void beforeClass() {
    assumeFalse("Does not work on Windows, because it uses UNIX shell commands or POSIX paths", Constants.WINDOWS);
    assumeFalse("FIXME: This test fails under Java 8 due to the Saxon dependency - see SOLR-1301", Constants.JRE_IS_MINIMUM_JAVA8);
    assumeFalse("FIXME: This test fails under J9 due to the Saxon dependency - see SOLR-1301", System.getProperty("java.vm.info", "<?>").contains("IBM J9"));
  }
  
  @Test
  public void testMapper() throws Exception {
    MorphlineMapper mapper = new MorphlineMapper();
    MapDriver<LongWritable, Text, Text, SolrInputDocumentWritable> mapDriver = MapDriver.newMapDriver(mapper);;

    Configuration config = mapDriver.getConfiguration();
    setupHadoopConfig(config);

    mapDriver.withInput(new LongWritable(0L), new Text("hdfs://localhost/" + DOCUMENTS_DIR + "/sample-statuses-20120906-141433.avro"));

    SolrInputDocument sid = new SolrInputDocument();
    sid.addField("id", "uniqueid1");
    sid.addField("user_name", "user1");
    sid.addField("text", "content of record one");
    SolrInputDocumentWritable sidw = new SolrInputDocumentWritable(sid);

    mapDriver
      .withCacheArchive(solrHomeZip.getAbsolutePath())
      .withOutput(new Text("0"), sidw);
    //mapDriver.runTest();
    List<Pair<Text, SolrInputDocumentWritable>> result = mapDriver.run();
    for (Pair<Text, SolrInputDocumentWritable> p: result) {
      System.out.println(p.getFirst());
      System.out.println(p.getSecond());
    }
  }
}
