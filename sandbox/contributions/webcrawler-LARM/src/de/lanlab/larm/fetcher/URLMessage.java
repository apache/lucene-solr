/*
 *  ====================================================================
 *  The Apache Software License, Version 1.1
 *
 *  Copyright (c) 2001 The Apache Software Foundation.  All rights
 *  reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in
 *  the documentation and/or other materials provided with the
 *  distribution.
 *
 *  3. The end-user documentation included with the redistribution,
 *  if any, must include the following acknowledgment:
 *  "This product includes software developed by the
 *  Apache Software Foundation (http://www.apache.org/)."
 *  Alternately, this acknowledgment may appear in the software itself,
 *  if and wherever such third-party acknowledgments normally appear.
 *
 *  4. The names "Apache" and "Apache Software Foundation" and
 *  "Apache Lucene" must not be used to endorse or promote products
 *  derived from this software without prior written permission. For
 *  written permission, please contact apache@apache.org.
 *
 *  5. Products derived from this software may not be called "Apache",
 *  "Apache Lucene", nor may "Apache" appear in their name, without
 *  prior written permission of the Apache Software Foundation.
 *
 *  THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 *  ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 *  USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 *  OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 *  SUCH DAMAGE.
 *  ====================================================================
 *
 *  This software consists of voluntary contributions made by many
 *  individuals on behalf of the Apache Software Foundation.  For more
 *  information on the Apache Software Foundation, please see
 *  <http://www.apache.org/>.
 */
package de.lanlab.larm.fetcher;

import java.net.*;
import java.io.*;
import de.lanlab.larm.util.URLUtils;
import de.lanlab.larm.net.URLNormalizer;
import de.lanlab.larm.net.HostManager;

/**
 * represents a URL which is passed around in the messageHandler
 *
 * @author    Administrator
 * @created   14. Juni 2002
 * @version   $Id$
 */
public class URLMessage implements Message, Serializable
{
    /**
     * the URL
     */
    protected URL url;

    /**
     * Description of the Field
     */
    protected volatile String urlString;

    /**
     * referer or null
     */
    protected URL referer;

    /**
     * externalized referer URL, to prevent multiple calls to url.toExternalForm()
     */
    protected volatile String refererString;

    /**
     * externalized referer URL, to prevent multiple calls to url.toExternalForm()
     */
    protected volatile String refererNormalizedString;

    /**
     * normalized URL, as defined by {@link de.lanlab.larm.net.URLNormalizer}
     * (lower case, index.* removed, all characters except alphanumeric ones escaped)
     */
    protected String normalizedURLString;


    boolean isFrame;

    /**
     * anchor text, as in &lt;a href="..."&gt;Anchor&lt;/a&gt;
     */
    protected String anchor;


    /**
     * Constructor for the URLMessage object
     *
     * @param url      Description of the Parameter
     * @param referer  Description of the Parameter
     * @param isFrame  Description of the Parameter
     * @param anchor   Description of the Parameter
     */
    public URLMessage(URL url, URL referer, boolean isFrame, String anchor, HostManager hostManager)
    {
        //super();
        this.url = url;
        this.urlString = url != null ? URLUtils.toExternalFormNoRef(url) : null;

        this.referer = referer;
        this.refererString = referer != null ? URLUtils.toExternalFormNoRef(referer) : null;
        this.refererNormalizedString = referer != null ? URLUtils.toExternalFormNoRef(URLNormalizer.normalize(referer, hostManager)) : null;
        this.isFrame = isFrame;
        this.anchor = anchor != null ? anchor : "";
        this.normalizedURLString = URLUtils.toExternalFormNoRef(URLNormalizer.normalize(url, hostManager));
        //this.normalizedURLString = URLNormalizer.
        //System.out.println("" + refererString + " -> " + urlString);
    }

    public String getNormalizedURLString()
    {
        return this.normalizedURLString;
    }

    /**
     * Gets the url attribute of the URLMessage object
     *
     * @return   The url value
     */
    public URL getUrl()
    {
        return this.url;
    }


    /**
     * Gets the referer attribute of the URLMessage object
     *
     * @return   The referer value
     */
    public URL getReferer()
    {
        return this.referer;
    }


    /**
     * Description of the Method
     *
     * @return   Description of the Return Value
     */
    public String toString()
    {
        return urlString;
    }


    /**
     * Gets the uRLString attribute of the URLMessage object
     *
     * @return   The uRLString value
     */
    public String getURLString()
    {
        return urlString;
    }


    /**
     * Gets the refererString attribute of the URLMessage object
     *
     * @return   The refererString value
     */
    public String getRefererString()
    {
        return refererString;
    }


    /**
     * Gets the anchor attribute of the URLMessage object
     *
     * @return   The anchor value
     */
    public String getAnchor()
    {
        return anchor;
    }


    /**
     * Description of the Method
     *
     * @return   Description of the Return Value
     */
    public int hashCode()
    {
        return url.hashCode();
    }


    /**
     * Description of the Method
     *
     * @param out              Description of the Parameter
     * @exception IOException  Description of the Exception
     */
    private void writeObject(java.io.ObjectOutputStream out)
        throws IOException
    {
        out.writeObject(url);
        out.writeObject(referer);
        out.writeBoolean(isFrame);
        out.writeUTF(anchor);
        out.writeUTF(refererNormalizedString);
        out.writeUTF(normalizedURLString);

    }


    /**
     * Description of the Method
     *
     * @param in                          Description of the Parameter
     * @exception IOException             Description of the Exception
     * @exception ClassNotFoundException  Description of the Exception
     */
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        url = (URL) in.readObject();
        referer = (URL) in.readObject();
        urlString = url.toExternalForm();
        refererString = referer.toExternalForm();
        isFrame = in.readBoolean();
        anchor = in.readUTF();
        refererNormalizedString = in.readUTF();
        normalizedURLString = in.readUTF();
    }


    /**
     * Gets the info attribute of the URLMessage object
     *
     * @return   The info value
     */
    public String getInfo()
    {
        return (referer != null ? refererString : "<start>") + "\t" + urlString + "\t" + this.getNormalizedURLString() + "\t" + (isFrame ? "1" : "0") + "\t" + anchor;
    }

}
