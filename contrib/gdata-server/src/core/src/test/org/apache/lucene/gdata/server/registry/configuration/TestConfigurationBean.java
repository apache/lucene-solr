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
package org.apache.lucene.gdata.server.registry.configuration;

import junit.framework.TestCase;

public class TestConfigurationBean extends TestCase {
    private ComponentConfiguration bean;

    protected void setUp() throws Exception {
        super.setUp();
        this.bean = new ComponentConfiguration();

    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.server.registry.configuration.ConfigurationBean.set(String,
     * String)'
     */
    public void testSet() {
        this.bean.set("field", "value");
        try {
            this.bean.set("field", "value");
            fail("field already set");
        } catch (IllegalArgumentException e) {
            //
        }
        assertEquals("value", this.bean.get("field"));
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.server.registry.configuration.ConfigurationBean.get(String)'
     */
    public void testGet() {
        assertNull(this.bean.get("field"));
        this.bean.set("field", "value");
        assertEquals("value", this.bean.get("field"));
    }

}
