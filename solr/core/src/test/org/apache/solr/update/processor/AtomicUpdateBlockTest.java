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

package org.apache.solr.update.processor;

import java.util.Collections;

import org.apache.solr.SolrTestCaseJ4Test;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtomicUpdateBlockTest extends SolrTestCaseJ4Test {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "true");
    initCore("solrconfig-update-processor-chains.xml", "schema-nest.xml"); // use "nest" schema
  }

  @Before
  public void before() {
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testBlockRealTimeGet() throws Exception {

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat_ss", new String[]{"aaa", "ccc"});
    doc.setField("child", Collections.singletonList(sdoc("id", "2", "cat_ss", "child")));
    addDoc(adoc(doc), "nested");

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    assertJQ(req("qt","/get","ids","1", "fl","id")
        ,"=={" +
            "  'response':{'numFound':1,'start':0,'docs':[" +
            "      {" +
            "        'id':'1'}]" +
            "  }}}"
    );

    assertU(commit());

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );

    // a cut-n-paste of the first big query, but this time it will be retrieved from the index rather than the transaction log
    assertJQ(req("qt","/get", "id","1", "fl","id, a_f,a_fd,a_fdS   a_fs,a_fds,a_fdsS,  a_d,a_dd,a_ddS,  a_ds,a_dds,a_ddsS,  a_i,a_id,a_idS   a_is,a_ids,a_idsS,   a_l,a_ld,a_ldS   a_ls,a_lds,a_ldsS")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"ccc\"], child:{\"id\":2,\"cat_ss\":[\"child\"]}" +
            "       }}"
    );

    assertJQ(req("qt","/get","id","1", "fl","id")
        ,"=={'doc':{'id':'1'}}"
    );
    assertJQ(req("qt","/get","ids","1", "fl","id")
        ,"=={" +
            "  'response':{'numFound':1,'start':0,'docs':[" +
            "      {" +
            "        'id':'1'}]" +
            "  }}}"
    );
  }
}
