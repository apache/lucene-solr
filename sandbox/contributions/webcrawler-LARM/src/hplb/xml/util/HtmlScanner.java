/*
 * $Id$
 *
 * Copyright 1997 Hewlett-Packard Company
 *
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.xml.util;

import hplb.org.xml.sax.HandlerBase;
import hplb.org.xml.sax.AttributeMap;
import hplb.org.xml.sax.XmlException;
import hplb.org.xml.sax.ErrorHandler;
import hplb.org.xml.sax.EntityHandler;
import hplb.org.xml.sax.DocumentHandler;
import hplb.xml.*;
import java.net.*;
import java.io.*;

/**
 * The HtmlScanner parses an HTML document for elements containing links.
 * For each link found it will invoke a client-provided callback method.
 * It knows about most HTML4.0 links and also knows about the &lt;base&gt;.
 *
 * <p>For an example use see UrlScanner.
 *
 * @see     HtmlObserver
 * @see     UrlScanner
 * @author  Anders Kristensen
 */
public class HtmlScanner extends HandlerBase {
    HtmlObserver observer;
    URL contextURL;
    Object data;
    Tokenizer tok;
    Reader in;

    /**
     * Parse the input on the specified stream as if it was HTML and
     * invoke the provided observer as links are encountered.
     * @param url   the URL to parse for links
     * @param observer  the callback object
     * @param data  client-specific data; this is passed back to the
     *              client in callbacks; this scanner doesn't use it
     * @throws Exception    see hplb.org.xml.sax.Parser.parse()
     * @see hplb.org.xml.sax.Parser.parse
     */
    public HtmlScanner(URL url, HtmlObserver observer ) throws Exception {
        this(new BufferedReader(new InputStreamReader(url.openStream())), url, observer);
    }

    /**
     * Parse the input on the specified stream as if it was HTML and
     * invoke the provided observer as links are encountered.
     * @param in    the input stream
     * @param url   the URL corresponding to this document
     * @param observer  the callback object
     * @throws Exception    see hplb.org.xml.sax.Parser.parse()
     * @see hplb.org.xml.sax.Parser.parse
	 * @deprecated
     */
    public HtmlScanner(InputStream in, URL url, HtmlObserver observer)
        throws Exception
    {
        this(new BufferedReader(new InputStreamReader(in)), url, observer, null);
    }

	    /**
     * Parse the input on the specified stream as if it was HTML and
     * invoke the provided observer as links are encountered.
     * @param in    the Reader
     * @param url   the URL corresponding to this document
     * @param observer  the callback object
     * @throws Exception    see hplb.org.xml.sax.Parser.parse()
     * @see hplb.org.xml.sax.Parser.parse
     */
    public HtmlScanner(Reader in, URL url, HtmlObserver observer)
        throws Exception
    {
        this(in, url, observer, null);
    }

	/**
     * Parse the input on the specified stream as if it was HTML and
     * invoke the provided observer as links are encountered.
	 * Although not deprecated, this method should not be used. Use HtmlScanner(Reader...) instead
	 * @deprecated
	 */
    public HtmlScanner(InputStream in, URL url, HtmlObserver observer, Object data)
        throws Exception
    {
		this(new BufferedReader(new InputStreamReader(in)), url, observer, data);
	}

    /**
     * Parse the input on the specified stream as if it was HTML and
     * invoke the provided observer as links are encountered.
     * @param in    the input stream
     * @param url   the URL corresponding to this document
     * @param observer  the callback object
     * @param data  client-specific data; this is passed back to the
     *              client in callbacks; this scanner doesn't use it
     * @throws Exception    see hplb.org.xml.sax.Parser.parse()
     * @see hplb.org.xml.sax.Parser.parse
     */
    public HtmlScanner(Reader in, URL url, HtmlObserver observer, Object data)
        throws Exception
    {
        this.in = in;
        this.observer = observer;
        this.contextURL = url;
        this.data = data;
        tok = new Tokenizer();
        setDocumentHandler(this);
        HTML.applyHacks(tok);
        tok.rcgnzEntities = false;
        tok.rcgnzCDATA = false;
        tok.atomize = true;
    }

    public void setDocumentHandler(DocumentHandler doc)
    {
        tok.setDocumentHandler(doc);
    }

    public void setEntityHandler(EntityHandler ent)
    {
        tok.setEntityHandler(ent);
    }

    public void setErrorHandler(ErrorHandler err)
    {
        tok.setErrorHandler(err);
    }

    public void parse() throws Exception
    {
        tok.parse(in);
    }

    public void startElement(String name, AttributeMap attributes) {
        String val;

        if (name == HTML.A) {
            if ((val = attributes.getValue("href")) != null) {
                observer.gotAHref(val, contextURL, data);
            }
        } else if (name == HTML.IMG) {
            if ((val = attributes.getValue("src")) != null) {
                observer.gotImgSrc(val, contextURL, data);
            }
        } else if (name == HTML.BASE) {
            if ((val = attributes.getValue("href")) != null) {
                observer.gotBaseHref(val, contextURL, data);
                if (contextURL != null) {
                    try {
                        contextURL = new URL(contextURL, val);
                    } catch (MalformedURLException ex) {
                        System.err.println("Bad <base> URL: " + val + ".");
                        System.err.println(ex.getMessage());
                    }
                }
            }
        } else if (name == HTML.AREA) {
            if ((val = attributes.getValue("href")) != null) {
                observer.gotAreaHref(val, contextURL, data);
            }
        } else if (name == HTML.FRAME) {
            if ((val = attributes.getValue("src")) != null) {
                observer.gotFrameSrc(val, contextURL, data);
            }
        }
    }
}
