package com.relevanz.indyo.contenthandler;

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

import java.util.Map;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Constructor;

import com.relevanz.indyo.util.IOUtils;

/**
 * Factory responsible for obtaining ContentHandlers.
 *
 * @author <a href="mailto:kelvint@apache.org">Kelvin Tan</a>
 * @version $Id$ 
 */
public abstract class FileContentHandlerFactory
{
    public static final String DEFAULT_HANDLER_KEY = "DEFAULT";
    static Category cat = Category.getInstance(FileContentHandlerFactory.class.getName());
    private static Map handlerRegistry;

    public static FileContentHandler getContentHandler(File f)
    {
        String extension = IOUtils.getFileExtension(f);
        if (handlerRegistry.containsKey(extension))
        {
            String handlerClassname = (String) handlerRegistry.get(extension);
            return (FileContentHandler) generateObject(handlerClassname,
                                                                     new Class[]{File.class},
                                                                     new Object[]{f});
        }
        else if (handlerRegistry.containsKey(DEFAULT_HANDLER_KEY))
        {
            String handlerClassname = (String) handlerRegistry.get(DEFAULT_HANDLER_KEY);
            return (FileContentHandler) generateObject(handlerClassname);
        }
        else
        {
            return NullHandler.getInstance();
        }
    }

    public static void setHandlerRegistry(Map handlerRegistry)
    {
        FileContentHandlerFactory.handlerRegistry = handlerRegistry;
    }

        /**
     * Utility method to return an object based on its class name.
     * The object needs to have a constructor which accepts no parameters.
     *
     * @param className  Class name of object to be generated
     * @return Object
     */
    private static Object generateObject(String className)
    {
        Object o = null;
        try
        {
            Class c = Class.forName(className);
            o = c.newInstance();
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
        return o;
    }

        /**
     * Utility method to return an object based on its class name.
     *
     * @param type  Class name of object to be generated
     * @param clazz Class array of parameters.
     * @param args Object array of arguments.
     * @return Object
     */
    private static Object generateObject(String className,
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
