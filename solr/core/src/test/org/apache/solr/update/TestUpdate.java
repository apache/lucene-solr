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
package org.apache.solr.update;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.noggit.ObjectBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.solr.core.SolrCore.verbose;

public class TestUpdate extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema15.xml");
  }

  @Test
  public void testUpdateableDocs() throws Exception {
    // The document may be retrieved from the index or from the transaction log.
    // Test both by running the same test with and without commits

    // do without commits
    doUpdateTest(new Callable() {
      @Override
      public Object call() throws Exception {
        return null;
      }
    });

    // do with commits
    doUpdateTest(new Callable() {
      @Override
      public Object call() throws Exception {
        commit("softCommit","false");
        return null;
      }
    });


  }

  public void doUpdateTest(Callable afterUpdate) throws Exception {
    clearIndex();
    afterUpdate.call();

    long version;

    version = addAndGetVersion(sdoc("id","1", "val_i",5), null);
    afterUpdate.call();
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",10)), null);
    afterUpdate.call();
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",5)), null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,*_i,*_is")
        ,"=={'doc':{'id':'1', 'val_i':5, 'val_is':[10,5]}}"
    );

    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",-1), "val_i",map("set",100)), null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,*_i,*_is")
        ,"=={'doc':{'id':'1', 'val_i':100, 'val_is':[10,5,-1]}}"
    );


    long version2;
    try {
      // try bad version added as a field in the doc
      version2 = addAndGetVersion(sdoc("id","1", "val_is",map("add",-100), "_version_",2), null);
      fail();
    } catch (SolrException se) {
      assertEquals(409, se.code());
    }

    try {
      // try bad version added as a request param
      version2 = addAndGetVersion(sdoc("id","1", "val_is",map("add",-100)), params("_version_","2"));
      fail();
    } catch (SolrException se) {
      assertEquals(409, se.code());
    }

    // try good version added as a field in the doc
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",-100), "_version_",version), null);
    afterUpdate.call();

    // try good version added as a request parameter
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",-200)), params("_version_",Long.toString(version)));
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,*_i,*_is")
        ,"=={'doc':{'id':'1', 'val_i':100, 'val_is':[10,5,-1,-100,-200]}}"
    );

    // extra field should just be treated as a "set"
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",-300), "val_i",2), null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,*_i,*_is")
        ,"=={'doc':{'id':'1', 'val_i':2, 'val_is':[10,5,-1,-100,-200,-300]}}"
    );

    // a null value should be treated as "remove"
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",-400), "val_i",null), null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,*_i,*_is")
        ,"=={'doc':{'id':'1', 'val_is':[10,5,-1,-100,-200,-300,-400]}}"
    );


    version = deleteAndGetVersion("1", null);
    afterUpdate.call();


    try {
      // test that updating a non-existing doc fails if we set _version_=1
      version2 = addAndGetVersion(sdoc("id","1", "val_is",map("add",-101), "_version_","1"), null);
      fail();
    } catch (SolrException se) {
      assertEquals(409, se.code());
    }


    // test that by default we can update a non-existing doc
    version = addAndGetVersion(sdoc("id","1", "val_i",102, "val_is",map("add",-102)), null);
    afterUpdate.call();
    assertJQ(req("qt","/get", "id","1", "fl","id,val*")
        ,"=={'doc':{'id':'1', 'val_i':102, 'val_is':[-102]}}"
    );

    version = addAndGetVersion(sdoc("id","1", "val_i",5), null);
    afterUpdate.call();

    version = addAndGetVersion(sdoc("id","1",
        "val_is",map("inc",1),
        "val2_i",map("inc","1"),
        "val2_f",map("inc",1),
        "val2_d",map("inc","1.0"),
        "val2_l",map("inc",1)
        ),
        null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,val*")
        ,"=={'doc':{'id':'1', 'val_i':5, 'val_is':[1], 'val2_i':1, 'val2_f':1.0, 'val2_d':1.0, 'val2_l':1}}"
    );

    version = addAndGetVersion(sdoc("id","1",
        "val_is",map("inc","-5"),
        "val2_i",map("inc",-5),
        "val2_f",map("inc","-5.0"),
        "val2_d",map("inc",-5),
        "val2_l",map("inc","-5")
    ),
        null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,val*")
        ,"=={'doc':{'id':'1', 'val_i':5, 'val_is':[-4], 'val2_i':-4, 'val2_f':-4.0, 'val2_d':-4.0, 'val2_l':-4}}"
    );

    version = addAndGetVersion(sdoc("id","1",
        "val_is",map("inc","2000000000"),
        "val2_i",map("inc",-2000000000),
        "val2_f",map("inc","1e+20"),
        "val2_d",map("inc",-1.2345678901e+100),
        "val2_l",map("inc","5000000000")
    ),
        null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,val*")
        ,"=={'doc':{'id':'1', 'val_i':5, 'val_is':[1999999996], 'val2_i':-2000000004, 'val2_f':1.0E20, 'val2_d':-1.2345678901e+100, 'val2_l':4999999996}}"
    );

  }

}
