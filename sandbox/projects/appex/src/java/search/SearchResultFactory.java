package search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.log4j.Category;
import org.apache.lucene.document.Document;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Factory class to materialize the objects which
 * are search results.
 */
public abstract class SearchResultFactory
{
    /**
     * Name of method for objects which don't have a broker, but which need to
     * be initialized somehow.
     */
    public static final String INIT_OBJECT_METHOD = "initializeObject";

    /**
     * Arguments for INIT_OBJECT_METHOD.
     */
    public static final Class[] INIT_OBJECT_METHOD_ARGS = new Class[]{String.class};

    public static final Class[] SEARCH_RESULT_WRAPPER_CTOR_ARGS
            = new Class[]{Object.class};

    private static Category cat = Category.getInstance(SearchResultFactory.class);

    /**
     * <p>
     * Materializes the object represented by the
     * {@link org.apache.lucene.document.Document}.
     * </p>
     * <p>
     * Objects which need to be returned via this method (essentially
     * any object which implements
     * {@link com.marketingbright.core.services.search.SearchResult},
     * must either have an associated broker, or a no-arg ctor and
     * INIT_OBJECT_METHOD with INIT_OBJECT_METHOD_ARGS.
     * </p>
     */
    public static Object getDocAsObject(Document doc) throws Exception
    {
        Class clazz = Class.forName(doc.get(DataSource.OBJECT_CLASS));
        String id = doc.get(DataSource.OBJECT_IDENTIFIER);
        Object o = null;
        try
        {
            Broker broker = BrokerFactory.getBroker(clazz);
            // assume it's a two-part compound pk if there's a comma
            int indexOfComma = id.indexOf(',');
            if (indexOfComma != -1)
            {
                String[] pks = new String[2];
                pks[0] = id.substring(0, indexOfComma);
                pks[1] = id.substring(indexOfComma + 1);
                o = broker.retrieveByPK(pks);
            }
            else
            {
                o = broker.retrieveByPK(id);
            }
        }
        catch (NoSuchBrokerException nsbe)
        {
            /**
             * Some objects don't have brokers or peers, this offers an
             * alternative.
             */
            o = clazz.newInstance();
            invokeMethod(o, INIT_OBJECT_METHOD,
                         INIT_OBJECT_METHOD_ARGS, new Object[]{id});
        }
        String searchResultClassname = null;
        searchResultClassname = doc.get(DataSource.SEARCH_RESULT_CLASSNAME);
        return generateObject(searchResultClassname,
                              SEARCH_RESULT_WRAPPER_CTOR_ARGS,
                              new Object[]{o});
    }

    /**
     * Utility method to invoke an object's method.
     * @param o Object to invoke the method on.
     * @param methodname Name of the method to invoke.
     * @param parameter Method parameters.
     * @param args Arguments the method requires.
     * @return Object returned by the method.
     */
    private static Object invokeMethod(Object o, String methodname,
                                       Class[] parameter, Object[] args)
    {
        Class c = o.getClass();
        try
        {
            Method m = c.getMethod(methodname, parameter);
            return m.invoke(o, args);
        }
        catch (NoSuchMethodException nsme)
        {
            cat.error(nsme.getMessage() + " This method doesn't exist..", nsme);
        }
        catch (IllegalAccessException iae)
        {
            cat.error("No access to " + iae.getMessage() + ".", iae);
        }
        catch (InvocationTargetException ite)
        {
            cat.error("Trouble invoking " + ite.getMessage(), ite);
        }
        return null;
    }

    /**
     * Utility method to return an object based on its class name.
     *
     * @param type  Class name of object to be generated
     * @param clazz Class array of parameters.
     * @param args Object array of arguments.
     * @return Object
     */
    public static Object generateObject(String className,
                                        Class[] clazz,
                                        Object[] args)
    {
        Object o = null;
        try
        {
            Class c = Class.forName(className);
            Constructor con = c.getConstructor(clazz);
            if (con != null)
            {
                o = con.newInstance(args);
            }
            else
                throw new InstantiationException("Constructor with arguments:" + clazz.toString() + " non-existent.");
        }
        catch (ClassNotFoundException cnfe)
        {
            cat.error(cnfe.getMessage() + " No class named '" + className + "' was found.", cnfe);
        }
        catch (InstantiationException ie)
        {
            cat.error(ie.getMessage() + " Class named '" + className + "' could not be  instantiated.", ie);
        }
        catch (IllegalAccessException iae)
        {
            cat.error(iae.getMessage() + " No access to class named '" + className + "'.", iae);
        }
        catch (NoSuchMethodException nsme)
        {
            cat.error(nsme.getMessage() + " No method in class named '" + className + "'.", nsme);
        }
        catch (InvocationTargetException ite)
        {
            cat.error(ite.getMessage() + " in class named '" + className + "'.", ite);
        }
        return o;
    }
}
