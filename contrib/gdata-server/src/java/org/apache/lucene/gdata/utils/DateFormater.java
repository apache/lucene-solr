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
import java.util.Locale;
import java.util.Stack;

/**
 * This class uses the {@link java.text.SimpleDateFormat} class to format dates
 * into strings according to given date pattern.
 * <p>
 * As the creation of <tt>SimpleDateFormat</tt> objects is quiet expensive and
 * formating dates is used quiet fequently the objects will be cached and reused
 * in subsequent calls.
 * </p>
 * <p>
 * This implementation is thread safe as it uses {@link java.util.Stack} as a
 * cache
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public class DateFormater {
    private final Stack<SimpleDateFormat> objectStack = new Stack<SimpleDateFormat>();

    private static final DateFormater formater = new DateFormater();

    /**
     * Date format as it is used in Http Last modified header (Tue, 15 Nov 1994
     * 12:45:26 GMT)
     */
    public static String HTTP_HEADER_DATE_FORMAT = "EEE, d MMM yyyy HH:mm:ss z";
    /**
     *  Date format as it is used in Http Last modified header (Tue, 15 Nov 1994
     * 12:45:26 +0000)
     */
    public static String HTTP_HEADER_DATE_FORMAT_TIME_OFFSET = "EEE, d MMM yyyy HH:mm:ss Z";

    private DateFormater() {
        super();
    }

    /**
     * Formats the given Date into the given date pattern.
     * 
     * @param date -
     *            the date to format
     * @param format -
     *            date pattern
     * @return - the string representation of the given <tt>Date</tt>
     *         according to the given pattern
     */
    public static String formatDate(final Date date, String format) {
        if (date == null || format == null)
            throw new IllegalArgumentException(
                    "given parameters must not be null");
        SimpleDateFormat inst = formater.getFormater();
        inst.applyPattern(format);
        formater.returnFomater(inst);
        return inst.format(date);
    }
    /**
     * Parses the given string into one of the specified formates
     * @param date - the string to parse
     * @param formates - formates
     * @return a {@link Date} instance representing the given string
     * @throws ParseException - if the string can not be parsed
     */
    public static Date parseDate(final String date, final String...formates) throws ParseException{
        for (int i = 0; i < formates.length; i++) {
            try {
             return parseDate(date,formates[i]);
            } catch (ParseException e) {
                //
            }
        }
        throw new ParseException("Unparseable date: "+date,0);
        
    }
    
    /**
     * Parses the given string into the specified formate
     * @param dateString - the string to parse
     * @param pattern - the expected formate
     * @return a {@link Date} instance representing the given string
     * @throws ParseException - if the string can not be parsed
     */
    public static Date parseDate(final String dateString,String pattern) throws ParseException{
        if(dateString == null|| pattern == null)
            throw new IllegalArgumentException(
            "given parameters must not be null");
        
        SimpleDateFormat inst = formater.getFormater();
        inst.applyPattern(pattern);
        return inst.parse(dateString);
    }

    private SimpleDateFormat getFormater() {
        if (this.objectStack.empty())
            return new SimpleDateFormat(DateFormater.HTTP_HEADER_DATE_FORMAT,Locale.ENGLISH);
        return this.objectStack.pop();
    }

    private void returnFomater(final SimpleDateFormat format) {
        if (this.objectStack.size() <= 25)
            this.objectStack.push(format);
    }

}
