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

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUpdate extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema15.xml");
  }

  @Test
  public void testUpdatableDocs() throws Exception {
    // The document may be retrieved from the index or from the transaction log.
    // Test both by running the same test with and without commits

    // do without commits
    doUpdateTest(() -> null);

    // do with commits
    doUpdateTest(() -> {
      assertU(commit("softCommit","false"));
      return null;
    });


  }

  public void doUpdateTest(@SuppressWarnings({"rawtypes"})Callable afterUpdate) throws Exception {
    clearIndex();
    afterUpdate.call();

    long version;

    version = addAndGetVersion(sdoc("id","1", "val_i",5, "copyfield_source","a"), null);
    afterUpdate.call();
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",10), "copyfield_source",map("add","b")), null);
    afterUpdate.call();
    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",5)), null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,*_i,*_is,copyfield_*")
        ,"=={'doc':{'id':'1', 'val_i':5, 'val_is':[10,5], 'copyfield_source':['a','b']}}"     // real-time get should not return stored copyfield targets
    );

    version = addAndGetVersion(sdoc("id","1", "val_is",map("add",-1), "val_i",map("set",100)), null);
    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,*_i,*_is")
        ,"=={'doc':{'id':'1', 'val_i':100, 'val_is':[10,5,-1]}}"
    );


    // Do a search to get all stored fields back and make sure that the stored copyfield target only
    // has one copy of the source.  This may not be supported forever!
    assertU(commit("softCommit","true"));
    assertJQ(req("q","*:*", "fl","id,*_i,*_is,copyfield_*")
        ,"/response/docs/[0]=={'id':'1', 'val_i':100, 'val_is':[10,5,-1], 'copyfield_source':['a','b'], 'copyfield_dest_ss':['a','b']}"
    );


    long version2;
    SolrException se = expectThrows(SolrException.class,
        () -> addAndGetVersion(sdoc("id","1", "val_is",map("add",-100), "_version_",2), null));
    assertEquals(409, se.code());

    // try bad version added as a request param
    se = expectThrows(SolrException.class,
        () -> addAndGetVersion(sdoc("id","1", "val_is",map("add",-100)), params("_version_","2")));
    assertEquals(409, se.code());

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

    // test that updating a non-existing doc fails if we set _version_=1
    se = expectThrows(SolrException.class,
        () -> addAndGetVersion(sdoc("id","1", "val_is",map("add",-101), "_version_","1"), null));
    assertEquals(409, se.code());

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


    // remove some fields
    version = addAndGetVersion(sdoc(
        "id", "1",
        "val_is", map("set",null),
        "val2_f", map("set",null)
    ),
        null);

    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,val*")
        ,"=={'doc':{'id':'1', 'val_i':5, 'val2_i':-2000000004, 'val2_d':-1.2345678901e+100, 'val2_l':4999999996}}"
    );

    // test that updating a unique id results in failure.
    ignoreException("Invalid update of id field");
    se = expectThrows(SolrException.class,
        () -> addAndGetVersion(
            sdoc("id", map("set","1"), "val_is", map("inc","2000000000")), null)
    );
    resetExceptionIgnores();
    assertEquals(400, se.code());
    assertTrue(se.getMessage().contains("Updating unique key, version or route field is not allowed"));

    afterUpdate.call();

    assertJQ(req("qt","/get", "id","1", "fl","id,val*")
        ,"=={'doc':{'id':'1', 'val_i':5, 'val2_i':-2000000004, 'val2_d':-1.2345678901e+100, 'val2_l':4999999996}}"
    );

   // nothing should have changed - check with a normal query that we didn't create a duplicate
    assertU(commit("softCommit","false"));
    assertJQ(req("q","id:1", "fl","id")
        ,"/response/numFound==1"
    );

  }

  @Test // SOLR-8866
  public void testUpdateLogThrowsForUnknownTypes() throws IOException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "444");
    doc.addField("text", new Object());//Object shouldn't be serialized later...

    AddUpdateCommand cmd = new AddUpdateCommand(req());
    cmd.solrDoc = doc;
    try {
      h.getCore().getUpdateHandler().addDoc(cmd); // should throw
    } catch (SolrException e) {
      if (e.getMessage().contains("serialize")) {
        return;//passed test
      }
      throw e;
    }
    fail();
  }

}
