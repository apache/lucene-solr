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

package de.lanlab.larm.util;

import java.net.URL;

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   27. Januar 2002
 */
public class URLUtils
{
    /**
     * does the same as URL.toExternalForm(), but leaves out the Ref part (which we would
     * cut off anyway) and handles the String Buffer so that no call of expandCapacity() will
     * be necessary
     * only meaningful if the default URLStreamHandler is used (as is the case with http, https, or shttp)
     *
     * @param u  the URL to be converted
     * @return   the URL as String
     */
    public static String toExternalFormNoRef(URL u)
    {
        String protocol  = u.getProtocol();
        String authority = u.getAuthority();
        String file      = u.getFile();

        StringBuffer result = new StringBuffer(
                    (protocol == null ? 0 : protocol.length()) +
                    (authority == null ? 0 : authority.length()) +
                    (file == null ? 1 : file.length()) + 3
                    );

        result.append(protocol);
        result.append(":");
        if (u.getAuthority() != null && u.getAuthority().length() > 0)
        {
            result.append("//");
            result.append(u.getAuthority());
        }
        if (u.getFile() != null && u.getFile().length() > 0)
        {
            result.append(u.getFile());
        }
        else
        {
            result.append("/");
        }

        return result.toString();
    }

}
