package de.lanlab.larm.net;

import java.util.*;
import xxl.collections.*;
import java.io.*;
import org.apache.commons.beanutils.*;
import java.lang.reflect.*;
import org.apache.commons.logging.*;

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




//class LRUCache
//{
//    HashMap cache = null;
//    LinkedList order = null;
//    int max;
//
//    public LRUCache(int max)
//    {
//
//        this.max = max;
//        cache = new HashMap((int)(max/0.6));
//        order = new LinkedList();
//    }
//
//    public Object get(Object key)
//    {
//        return cache.get(key);
//    }
//
//
//
//    public void put(Object key, Object value)
//    {
//        if(!cache.containsKey(key))
//        {
//           if(order.size() > max)
//           {
//               cache.remove(order.removeLast());
//           }
//        }
//        else
//        {
//            //assert order.contains(key);
//            order.remove(key);
//            // quite expensive, probably need a hashed list
//            // or something even simpler
//        }
//        order.addFirst(key);
//        cache.put(key, value);
//    }
//}

/**
 * Uses @link{#resolveHost()} which transforms a host name according to the rules
 * Rules are (and executed in this order)
 * <ul>
 * <li>if host starts with (startsWith), replace this part with (replacement)
 * <li>if host ends with (endsWith), replace it with (replacement)
 * <li>if host is (synonym), replace it with (replacement)
 * </ul>
 * the resolver can be configured through a property file, which is loaded by an
 * Apache BeanUtils property loader.<p>
 * Actually the resolver doesn't do any network calls, so this class can be used
 * with any string, if you really need to
 * @author Clemens Marschner
 * @version 1.0
 */
public class HostResolver
{

    HashMap synonym;
    public HostResolver()
    {
        synonym = new HashMap();
    }

    /**
     * convenience method that loads the config from a properties file
     * @param fileName a property file
     * @throws IOException thrown if fileName is wrong or something went wrong while reading
     * @throws InvocationTargetException thrown by java.util.Properties
     * @throws IllegalAccessException thrown by java.util.Properties
     */
    public void initFromFile(String fileName) throws IOException, InvocationTargetException, IllegalAccessException
    {
        InputStream in = new FileInputStream(fileName);
        Properties p = new Properties();
        p.load(in);
        in.close();
        initFromProperties(p);
    }

    /**
     * populates the synonym, startsWith and endsWith properties with a BeanUtils.populate()
     * @param props
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public void initFromProperties(Properties props) throws InvocationTargetException, IllegalAccessException
    {
        BeanUtils.populate(this, props);
    }

    ArrayList startsWithArray = new ArrayList();
    int startsWithSize = 0;
    ArrayList endsWithArray = new ArrayList();
    int endsWithSize = 0;

    public String getStartsWith(String name) throws IllegalAccessException
    {
        throw new IllegalAccessException("brrffz");
    }

    public void setStartsWith(String name, String rep)
    {
        addHostStartsWithReplace(name.replace(',','.'), rep.replace(',','.'));
    }
    public String getEndsWith(String name) throws IllegalAccessException
    {
        throw new IllegalAccessException("brrffz");
    }
    public void setEndsWith(String name, String rep)
    {
        this.addHostEndsWithReplace(name.replace(',','.'), rep.replace(',','.'));
    }

    public void setSynonym(String name, String syn)
    {
        addSynonym(name.replace(',','.'), syn.replace(',','.'));
    }
    public String getSynonym(String name) throws IllegalAccessException
    {
        throw new IllegalAccessException("brrffz");
    }
    public void addSynonym(String name, String syn)
    {
        System.out.println("adding synonym " + name + " -> " + syn);
        synonym.put(name, syn);
    }

    /**
     * transforms a host name if a rule is found
     * @param hostName
     * @return probably changed host name
     */
    public String resolveHost(String hostName)
    {
        if(hostName == null)
        {
            return null;
        }
        for(int i=0; i<startsWithSize; i++)
        {
            String[] test = (String[])startsWithArray.get(i);
            if(hostName.startsWith(test[0]))
            {
                hostName = test[1] + hostName.substring(test[0].length());
                break;
            }
        }
        for(int i=0; i<endsWithSize; i++)
        {
            String[] test = (String[])endsWithArray.get(i);
            if(hostName.endsWith(test[0]))
            {
                hostName =  hostName.substring(0, hostName.length() - test[0].length()) + test[1];
                break;
            }
        }
        String syn = (String)synonym.get(hostName);
        return syn != null ? syn : hostName;
    }

    public void addHostStartsWithReplace(String startsWith, String replace)
    {
        System.out.println("adding sw replace " + startsWith + " -> " + replace);
        startsWithArray.add(new String[] { startsWith, replace });
        startsWithSize++;
    }

    public void addHostEndsWithReplace(String endsWith, String replace)
    {
        System.out.println("adding ew replace " + endsWith + " -> " + replace);
        endsWithArray.add(new String[] { endsWith, replace });
        endsWithSize++;
    }

//    /** The pattern cache to compile and store patterns */
//    private PatternCache __patternCache;
//    /** The hashtable to cache higher-level expressions */
//    private Cache __expressionCache;
//    /** The pattern matcher to perform matching operations. */
//    private Perl5Matcher __matcher = new Perl5Matcher();
//
//    public void addReplaceRegEx(String findRegEx, String replaceRegEx, boolean greedy)
//    {
//        int compileOptions    = Perl5Compiler.CASE_INSENSITIVE_MASK;
//        int numSubstitutions = 1;
//        if(greedy)
//        {
//            numSubstitutions = Util.SUBSTITUTE_ALL;
//        }
//
//        Pattern compiledPattern = __patternCache.getPattern(findRegEx, compileOptions);
//        Perl5Substitution substitution = new Perl5Substitution(replaceRegEx, numInterpolations);
//        ParsedSubstitutionEntry entry = new ParsedSubstitutionEntry(compiledPattern, substitution,  numSubstitutions);
//        __expressionCache.addElement(expression, entry);
//
//        result = Util.substitute(__matcher, compiledPattern, substitution,
//                     input, numSubstitutions);
//
//        __lastMatch = __matcher.getMatch();
//
//        return result;
//    }

}