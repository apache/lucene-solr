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
package org.apache.lucene.gdata.utils;

import org.apache.lucene.gdata.search.analysis.PlainTextStrategy;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.utils.ReflectionUtils.ReflectionException;
import org.apache.lucene.search.RangeQuery;

import junit.framework.TestCase;

public class TestReflectionUtils extends TestCase {

    /*
     * Test method for 'org.apache.lucene.gdata.utils.ReflectionUtils.implementsType(Class, Class)'
     */
    public void testImplementsType() {

    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.ReflectionUtils.extendsType(Class, Class)'
     */
    public void testExtendsType() {
        assertTrue(ReflectionUtils.isTypeOf(Integer.class,Number.class));
        assertFalse(ReflectionUtils.isTypeOf(null,CharSequence.class));
    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.ReflectionUtils.isTypeOf(Class, Class)'
     */
    public void testIsTypeOf() {
        assertTrue(ReflectionUtils.isTypeOf(String.class,CharSequence.class));
        assertTrue(ReflectionUtils.isTypeOf(Integer.class,Number.class));
        assertFalse(ReflectionUtils.isTypeOf(Integer.class,CharSequence.class));
        assertFalse(ReflectionUtils.isTypeOf(null,CharSequence.class));
        assertFalse(ReflectionUtils.isTypeOf(Integer.class,null));

    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.ReflectionUtils.getDefaultInstance(Class<T>) <T>'
     */
    public void testGetDefaultInstance() {
        assertEquals(new String(),ReflectionUtils.getDefaultInstance(String.class));
        try{
        ReflectionUtils.getDefaultInstance(Integer.class);
        fail("can not create instance");
        }catch (ReflectionException e) {
            
        }
        try{
            ReflectionUtils.getDefaultInstance(null);
            fail("can not create instance");
            }catch (ReflectionException e) {
                
            }
    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.ReflectionUtils.canCreateInstance(Class)'
     */
    public void testCanCreateInstance() {
        assertTrue(ReflectionUtils.canCreateInstance(String.class));
        assertFalse(ReflectionUtils.canCreateInstance(Integer.class));
        assertFalse(ReflectionUtils.canCreateInstance(Integer.TYPE));
        assertFalse(ReflectionUtils.canCreateInstance(null));
        
        
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.utils.ReflectionUtils.getPrimitiveWrapper(Class)'
     */
    public void testGetPrimitiveWrapper() {
        
        assertEquals(Integer.class,ReflectionUtils.getPrimitiveWrapper(Integer.TYPE));
        assertEquals(Long.class,ReflectionUtils.getPrimitiveWrapper(Long.TYPE));
        assertEquals(Float.class,ReflectionUtils.getPrimitiveWrapper(Float.TYPE));
        assertEquals(Byte.class,ReflectionUtils.getPrimitiveWrapper(Byte.TYPE));
        assertEquals(Double.class,ReflectionUtils.getPrimitiveWrapper(Double.TYPE));
        assertEquals(Short.class,ReflectionUtils.getPrimitiveWrapper(Short.TYPE));
        assertEquals(Boolean.class,ReflectionUtils.getPrimitiveWrapper(Boolean.TYPE));
        try {
        ReflectionUtils.getPrimitiveWrapper(null);
        fail("type is null");
        }catch (ReflectionException e) {
            
        }
        try {
            ReflectionUtils.getPrimitiveWrapper(String.class);
            fail("type is not a primitive");
            }catch (ReflectionException e) {
                
            }
        
    }
    
    public void testHasdesiredconstructor(){
        assertFalse(ReflectionUtils.hasDesiredConstructor(PlainTextStrategy.class, new Class[]{IndexSchemaField.class}));
        assertFalse(ReflectionUtils.hasDesiredConstructor(PlainTextStrategy.class, new Class[]{}));
        assertTrue(ReflectionUtils.hasDesiredConstructor(String.class, new Class[]{String.class}));
    }

}
