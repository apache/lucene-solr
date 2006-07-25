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
