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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.TestCase;

public class TestDateFormater extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }
    /*
     * Test method for 'org.apache.lucene.gdata.utils.DateFormater.formatDate(Date, String)'
     */
    public void testFormatDate() throws ParseException {
        // this reg. --> bit weak but does the job
            java.util.regex.Pattern pattern =  java.util.regex.Pattern.compile("[A-Z][a-z]{1,2}, [0-9]{1,2} [A-Z][a-z]{2} [0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} [A-Z]{2,4}");
            Date date = new Date();
            String formatedDate = DateFormater.formatDate(date,DateFormater.HTTP_HEADER_DATE_FORMAT);
            assertTrue(pattern.matcher(formatedDate).matches());    
            DateFormater.parseDate("Sun, 25 Jun 2006 13:51:23 +0000",DateFormater.HTTP_HEADER_DATE_FORMAT,DateFormater.HTTP_HEADER_DATE_FORMAT_TIME_OFFSET);
            DateFormater.parseDate("Sun, 25 Jun 2006 13:51:23 CEST",DateFormater.HTTP_HEADER_DATE_FORMAT,DateFormater.HTTP_HEADER_DATE_FORMAT_TIME_OFFSET);
            //TODO extend this
    }
    
    public void testFormatDateStack(){
        DateFormater formater = new DateFormater();
        SimpleDateFormat f1 = formater.getFormater();
        SimpleDateFormat f2 = formater.getFormater();
        assertNotSame(f1,f2);
        formater.returnFomater(f1);
        assertSame(f1,formater.getFormater());
        formater.returnFomater(f2);
        assertSame(f2,formater.getFormater());
        
    }
    
}
