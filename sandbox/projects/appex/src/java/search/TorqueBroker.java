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
import org.apache.torque.om.ComboKey;
import org.apache.torque.om.ObjectKey;
import org.apache.torque.om.StringKey;
import org.apache.torque.pool.DBConnection;
import org.apache.torque.util.Criteria;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Torque implementation of the Broker interface.
 *
 * @author <a href="mailto:soonping@relevanz.com">Phang Soon-Ping</a>
 */
public class TorqueBroker implements Broker
{
    protected static Map classMap = new Hashtable();
    private static Category cat = Category.getInstance(TorqueBroker.class);
    private static final String PEER_SUFFIX = "Peer";

    protected Object peer;

    public TorqueBroker(Class bObj) throws NoSuchBrokerException
    {
        String className = bObj.getName();
        peer = classMap.get(bObj);
        if (peer == null)
        {
            String peerClassName = className + PEER_SUFFIX;
            try
            {
                peer = Class.forName(peerClassName).newInstance();
                classMap.put(bObj, peer);
            }
            catch (Exception e)
            {
                throw new NoSuchBrokerException("Unable to obtain a broker for "
                                                + bObj.getName() + '.');
            }
        }
    }

    public synchronized List doSelect(Criteria crit) throws Exception
    {
        return (List) invokeMethod(
                peer,
                "doSelect",
                new Class[]{Criteria.class},
                new Object[]{crit}
        );
    }

    public synchronized List doSelect(Criteria crit, DBConnection dbCon)
            throws Exception
    {
        return (List) invokeMethod(
                peer,
                "doSelect",
                new Class[]{Criteria.class, DBConnection.class},
                new Object[]{crit, dbCon}
        );
    }

    public synchronized Object getSingleObject(Criteria crit)
            throws ObjectNotFoundException, Exception
    {
        List matchingObjects = doSelect(crit);
        if (!matchingObjects.isEmpty())
        {
            return matchingObjects.get(0);
        }
        else
        {
            throw new ObjectNotFoundException(crit);
        }
    }

    public synchronized Object retrieveByPK(String[] pk) throws Exception
    {
        ObjectKey oKey = null;
        if (pk.length > 1)
        {
            StringKey[] strKeys = new StringKey[pk.length];
            for (int i = 0; i < pk.length; i++)
            {
                strKeys[i] = new StringKey(pk[i]);
            }
            oKey = new ComboKey(strKeys);
        }
        else
        {
            oKey = new StringKey(pk[0]);
        }
        return invokeMethod(
                peer,
                "retrieveByPK",
                new Class[]{ObjectKey.class},
                new Object[]{oKey});
    }

    public synchronized Object retrieveByPK(String pk) throws Exception
    {
        ObjectKey oKey = new StringKey(pk);
        return invokeMethod(
                peer,
                "retrieveByPK",
                new Class[]{ObjectKey.class},
                new Object[]{oKey});
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
}
