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

package de.lanlab.larm.fetcher;

import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Pattern;
import de.lanlab.larm.util.*;

/**
 * filter class. Tries to match a regular expression with an incoming URL
 * @author Clemens Marschner
 * @version $Id$
 */
class URLScopeFilter extends Filter implements MessageListener
{
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
      this.messageHandler = handler;
    }
    MessageHandler messageHandler;

    /**
     * the regular expression which describes a valid URL
     */
    private Pattern pattern;
    private Perl5Matcher matcher;
    private Perl5Compiler compiler;
    SimpleLogger log;

    public URLScopeFilter(SimpleLogger log)
    {
            matcher = new Perl5Matcher();
            compiler = new Perl5Compiler();
            this.log = log;
    }

    public String getRexString()
    {
        return pattern.toString();
    }

    /**
     * set the regular expression
     * @param rexString the expression
     */
    public void setRexString(String rexString) throws org.apache.oro.text.regex.MalformedPatternException
    {
        this.pattern = compiler.compile(rexString, Perl5Compiler.CASE_INSENSITIVE_MASK | Perl5Compiler.SINGLELINE_MASK);
        //System.out.println("pattern set to: " + pattern);
    }


    /**
     * this method will be called by the message handler. Tests the URL
     * and throws it out if it's not in the scope
     */
    public Message handleRequest(Message message)
    {
        if(message instanceof URLMessage)
        {
            String urlString = ((URLMessage)message).getNormalizedURLString();
            int length = urlString.length();
            char buffer[] = new char[length];
            urlString.getChars(0,length,buffer,0);

            //System.out.println("using pattern: " + pattern);
            boolean match = matcher.matches(buffer, pattern);
            if(!match)
            {
                //log.log("URLScopeFilter: not in scope: " + urlString);
                log.log(message.toString());
                filtered++;

                return null;
            }
        }
        return message;
    }

}