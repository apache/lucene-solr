/**
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
package org.apache.lucene.gdata.search.analysis;

import javax.xml.xpath.XPathExpressionException;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.server.registry.ProvidedServiceConfig;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;

import com.google.gdata.data.Category;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.data.HtmlTextConstruct;
import com.google.gdata.data.extensions.EventEntry;

public class TestDomIndexable extends TestCase {

   
    public void testConstructor(){
        try {
            new DomIndexable(new ServerBaseEntry());
            fail("no service config");
        } catch (NotIndexableException e) {
            
            
        }
        ServerBaseEntry e = new ServerBaseEntry();
        e.setServiceConfig(new ProvidedServiceConfig());
        try {
            new DomIndexable(e);
            fail("no extension profile");
        } catch (IllegalStateException e1) {
            
            
        } catch (NotIndexableException e2) {
            
            fail("unexp. exception");   
        }
        e.setServiceConfig(new ProvidedServiceStub());
        try {
            new DomIndexable(e);
        } catch (NotIndexableException e1) {
         fail("unexp. exception");   
         
        }
    }
    /*
     * Test method for 'org.apache.lucene.gdata.search.analysis.DomIndexable.applyPath(String)'
     */
    public void testApplyPath() throws NotIndexableException, XPathExpressionException {
        String content = "fooo bar<br>";
        ServerBaseEntry entry = new ServerBaseEntry();
        entry.setContent(new HtmlTextConstruct(content));
        entry.setServiceConfig(new ProvidedServiceStub());
        
            Indexable ind = new DomIndexable(entry);
            Node n = ind.applyPath("/entry/content");
            assertNotNull(n);
            assertEquals(content,n.getTextContent());
            Node attr = ind.applyPath("/entry/content/@type");
            assertNotNull(attr);
            assertEquals("html",attr.getTextContent());
            assertTrue(attr instanceof Attr);
            
    }
    
}
