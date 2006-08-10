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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.queryParser.QueryParser.Operator;

import junit.framework.TestCase;

public class TestGdataQueryParser extends TestCase {

    
    public void testConstructor(){
        String field = "someField";
        IndexSchema s = new IndexSchema();
        s.setDefaultSearchField(field);
        GDataQueryParser p = new GDataQueryParser(s);
        assertEquals(field,p.getField());
        assertEquals(Operator.AND,p.getDefaultOperator());
        assertEquals(StandardAnalyzer.class,p.getAnalyzer().getClass());
    }
}
