/** 
 * Copyright 2004 The Apache Software Foundation 
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
package org.apache.lucene.gdata.search.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.RangeQuery;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 *
 */
public class TestQueryTranslator extends TestCase {
    private static final String CONTENT_FIELD = "content";
    private static final String UPDATED_FIELD = "updated";
    private IndexSchema schema;
    Map<String,String[]> parameterMap;
    /*
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        this.schema = new IndexSchema();
        //must be set
        this.schema.setDefaultSearchField(CONTENT_FIELD);
        this.schema.setIndexLocation("/tmp/");
        this.schema.setName(ProvidedServiceStub.SERVICE_NAME);
        IndexSchemaField field = new IndexSchemaField();
        
        field.setName(CONTENT_FIELD);
        field.setContentType(ContentType.TEXT);
        
        IndexSchemaField field1 = new IndexSchemaField();
        field1.setName(UPDATED_FIELD);
        field1.setContentType(ContentType.GDATADATE);
        this.schema.addSchemaField(field);
        this.schema.addSchemaField(field1);
        parameterMap = new HashMap<String,String[]>();
        

    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.query.QueryTranslator.translateHttpSearchRequest(IndexSchema, Map<String, String>, String, String)'
     */
    public void testTranslateHttpSearchRequest() {
        assertNull(QueryTranslator.translateHttpSearchRequest(this.schema,this.parameterMap,null));
        String contentQuery = "content1 -content2 \"exact Content\""; 
        parameterMap.put("q", new String[]{contentQuery});
        String expected = CONTENT_FIELD+":("+contentQuery+") ";
      
        assertEquals(expected,QueryTranslator.translateHttpSearchRequest(this.schema,this.parameterMap,null));
        parameterMap.put("updated-min", new String[]{"2005-08-09T10:57:00-08:00"});
        parameterMap.put("updated-max", new String[]{"2005-10-09T10:57:00-08:00"});
        parameterMap.put("max-results", new String[]{"3"});
        parameterMap.remove("q");
        parameterMap.put(CONTENT_FIELD, new String[]{"apache"});
        
        String tranlatedQuery = QueryTranslator.translateHttpSearchRequest(this.schema,this.parameterMap,"test |{urn:google.com} {urn:apache.org}");
        assertTrue(tranlatedQuery.contains("updated:[1123613820000 TO 1128884219999]"));
        assertTrue(tranlatedQuery.contains(CONTENT_FIELD+":(apache)"));
        
        parameterMap.remove("updated-max");
        tranlatedQuery = QueryTranslator.translateHttpSearchRequest(this.schema,this.parameterMap,"test |{urn:google.com} {urn:apache.org}");
        assertTrue(tranlatedQuery.contains("updated:[1123613820000 TO "+Long.MAX_VALUE+"]"));
        assertTrue(tranlatedQuery.contains(CONTENT_FIELD+":(apache)"));
        parameterMap.put("updated-max", new String[]{"2005-10-09T10:57:00-08:00"});
        parameterMap.remove("updated-min");
        tranlatedQuery = QueryTranslator.translateHttpSearchRequest(this.schema,this.parameterMap,"test |{urn:google.com} {urn:apache.org}");
        assertTrue(tranlatedQuery.contains("updated:["+0+" TO 1128884219999]"));
        assertTrue(tranlatedQuery.contains(CONTENT_FIELD+":(apache)"));
        
        
        
        parameterMap.put("wrong-parameter", new String[]{"3"});
        try{
        QueryTranslator.translateHttpSearchRequest(this.schema,this.parameterMap,"test |{urn:google.com} {urn:apache.org}");
        fail("illegal parameter");
        }catch (RuntimeException e) {
           
        }
       
    }

}
