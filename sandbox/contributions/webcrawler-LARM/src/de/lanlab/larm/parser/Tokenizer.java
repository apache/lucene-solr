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

package de.lanlab.larm.parser;

import java.util.Dictionary;
import java.util.Hashtable;
import java.io.*;
import java.net.URL;

/**
 * This parser is based on HEX, the HTML enabled XML parser, written by
 * Anders Kristensen, HP Labs Bristol.
 * It was stripped down and specialized to handle links in HTML pages. I removed
 * some bugs. And it's FAST, about 10 x faster than the original HEX parser.
 * Being some sort of SAX parser it calls the callback functions of the LinkHandler
 * when links are found.
 * Attention: This parser is not thread safe, as a lot of locks were removed
 *
 * @author    Clemens Marschner
 * $Id$
 */

public class Tokenizer
{
    /**
     * Sets the entityHandler attribute of the Tokenizer object
     *
     * @param e  The new entityHandler value
     *
    public void setEntityHandler(EntityHandler e) { }
    */

    /**
     * Sets the errorHandler attribute of the Tokenizer object
     *
     * @param e  The new errorHandler value
     *
    public void setErrorHandler(hplb.org.xml.sax.ErrorHandler e) { }
    */

    /**
     * Sets the documentHandler attribute of the Tokenizer object
     *
     * @param e  The new documentHandler value
     *
    public void setDocumentHandler(hplb.org.xml.sax.DocumentHandler e) { }
    */

    // FSM states:
    final static int ST_START = 1;
    final static int ST_TAG_LT = 3;
    final static int ST_TAG_NAME = 4;
    final static int ST_TAG_WS = 5;
    final static int ST_EMPTY_TAG_SLASH = 6;
    final static int ST_NAME = 7;
    final static int ST_NAME_WS = 8;
    final static int ST_EQ = 9;
    final static int ST_VALUE = 10;
    final static int ST_VALUE_QUOTED = 11;
    final static int ST_PCDATA = 21;
    final static int ST_COMMENT = 22;
    final static int ST_IN_ANCHOR = 23;

    LinkHandler linkHandler;

    String sysID = "what's this?";

    /**
     * Description of the Field
     */
    protected Hashtable noCaseElms;
    /**
     * Description of the Field
     */
    public boolean rcgnzWS = true;
    // is white space chars recognized as PCDATA
    // even when preceeding tags?
    /**
     * Description of the Field
     */
    public boolean rcgnzEntities = true;
    /**
     * Description of the Field
     */
    public boolean rcgnzCDATA = true;
    /**
     * Description of the Field
     */
    public boolean rcgnzComments = true;
    //
    /**
     * Description of the Field
     */
    public boolean atomize = false;
    // make element and attr names atoms

    private final static int ATTR_HREF = 1;
    private final static int ATTR_SRC = 2;

    private final static int LINKTYPE_NONE = 0;
    private final static int LINKTYPE_A = 1;
    private final static int LINKTYPE_BASE = 2;
    private final static int LINKTYPE_FRAME = 3;
    private final static int LINKTYPE_AREA = 4;


    private byte linkTagType;
    private boolean linkAttrFound;
    private int linkAttrType;
    private String linkValue;
    private boolean keepPCData;
    private boolean isInTitleTag;
    private boolean isInAnchorTag;
    SimpleCharArrayWriter buf = new SimpleCharArrayWriter();
    boolean isStartTag = true;
    /**
     * Signals whether a non-empty element has any children. If not we must
     * generate an artificial empty-string child [characters(buf, 0, 0)].
     */
    boolean noChildren;
    SimpleCharArrayWriter tagname = new SimpleCharArrayWriter();
    SimpleCharArrayWriter attrName = new SimpleCharArrayWriter();
    SimpleCharArrayWriter attrValue = new SimpleCharArrayWriter(1000);
    SimpleCharArrayWriter pcData = new SimpleCharArrayWriter(8000);
    int pcDataLength;

    /**
     * specifies how much pcData is kept when a link was found
     */
    private final static int MAX_PCDATA_LENGTH = 200;

    Reader in;

    /**
     * Description of the Field
     */
    public final EntityManager entMngr = new EntityManager(this);
    /**
     * Description of the Field
     */
    protected int state = ST_START;
    /**
     * Description of the Field
     */
    protected int qchar;


    // <'> or <"> when parsing quoted attr values


    /**
     * Constructor for the Tokenizer object
     */
    public Tokenizer() { }


    /**
     * Sets the linkHandler attribute of the Tokenizer object
     *
     * @param handler  The new linkHandler value
     */
    public void setLinkHandler(LinkHandler handler)
    {
        linkHandler = handler;
    }


    /**
     * Description of the Method
     *
     * @param publicID       Description of the Parameter
     * @param sysID          Description of the Parameter
     * @exception Exception  Description of the Exception
     */
    public void parse(String publicID, String sysID)
        throws Exception
    {
        this.sysID = sysID;
        parse(new URL(sysID).openStream());
    }


    /**
     * Description of the Method
     *
     * @param in             Description of the Parameter
     * @exception Exception  Description of the Exception
     */
    public void parse(InputStream in)
        throws Exception
    {
        parse(new BufferedReader(new InputStreamReader(in)));
    }


    /**
     * Description of the Method
     *
     * @param in             Description of the Parameter
     * @exception Exception  Description of the Exception
     */
    public void parse(Reader in)
        throws Exception
    {
        if (linkHandler == null)
        {
            throw new IllegalStateException("parse called without LinkHandler being set");
        }

        this.in = in;
        toStart();
        tokenize();
    }


    /**
     * Description of the Method
     *
     * @param elementName  Description of the Parameter
     */
    public void ignoreCase(String elementName)
    {
        if (noCaseElms == null)
        {
            noCaseElms = new Hashtable();
        }
        noCaseElms.put(elementName.toLowerCase(), elementName);
    }


    /**
     * Description of the Method
     *
     * @param b  Description of the Parameter
     */
    public void rcgnzWS(boolean b)
    {
        rcgnzWS = b;
    }


    // invoked after doing any Handler callback - resets state
    /**
     * Description of the Method
     */
    protected void toStart()
    {
        state = ST_START;
        buf.reset();
        tagname.reset();
        attrName.reset();
        attrValue.reset();
        pcData.reset();
        pcDataLength = 0;
        //attrs.clear();
        isStartTag = true;
        isInAnchorTag = false;

        // until proven wrong

        linkTagType = LINKTYPE_NONE;
        linkAttrFound = false;
        linkAttrType = 0;
        linkValue = "";
        //keepPCData= false;
    }


    /**
     * Description of the Method
     *
     * @exception Exception  Description of the Exception
     */
    public void tokenize()
        throws Exception
    {
        int c;


        while ((c = read()) != -1)
        {
            switch (state)
            {
                case ST_START:
                    switch (c)
                    {
                        case '<':
                            state = ST_TAG_LT;
                            linkTagType = LINKTYPE_NONE;
                            linkAttrFound = false;
                            linkAttrType = 0;
                            linkValue = "";

                            isStartTag = true;
                            keepPCData= false;

                            // until proven wrong
                            tagname.reset();
                            break;
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            if (!rcgnzWS)
                            {
                                break;
                            }
                        // else fall through
                        default:
                            state = ST_PCDATA;
                            if(keepPCData)
                            {
                                pcData.write(c);
                                pcDataLength++;
                                if(pcDataLength > MAX_PCDATA_LENGTH)
                                {
                                    this.gotPCDATA(false);
                                    keepPCData = false;
                                }
                            }

                    }
                    break;
                case ST_PCDATA:
                    if (c == '<')
                    {
                        if(keepPCData)
                        {
                            gotPCDATA(true);
                            keepPCData = false;
                        }
                        linkTagType = LINKTYPE_NONE;
                        linkAttrFound = false;
                        linkAttrType = 0;
                        linkValue = "";
                        state = ST_TAG_LT;
                    }
                    else
                    {
                        if(keepPCData)
                        {
                            pcData.write(c);
                            pcDataLength++;
                            if(pcDataLength > MAX_PCDATA_LENGTH)
                            {
                                this.gotPCDATA(false);
                                keepPCData = false;
                            }

                        }
                    }
                    break;
                case ST_TAG_LT:
                    switch (c)
                    {
                        case '/':
                            isStartTag = false;
                            state = ST_TAG_NAME;
                            break;
                        case '!':
                            c = read();
                            if ((c == '-' && !rcgnzComments) || (c == '[' && !rcgnzCDATA))
                            {
                                state = ST_PCDATA;
                                pcData.reset();
                                pcData.write(c);
                                break;
                            }
                            if (c == '-')
                            {
                                state = ST_COMMENT;
                            }
                            else if (c == '[')
                            {
                                parseCDATA();
                            }
                            else
                            {
                                // FIXME: shouldn't be delivered as PCDATA
                                //warning("Bad markup " + buf);
                                state = ST_PCDATA;
                                pcData.reset();
                                pcData.write(c);
                            }
                            break;
                        case '?':
                            parsePI();
                            break;
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            state = ST_TAG_WS;
                            break;
                        default:
                            tagname.write(Character.toLowerCase((char) c));
                            // ## changed
                            state = ST_TAG_NAME;
                    }
                    break;
                case ST_TAG_NAME:
                    switch (c)
                    {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            state = ST_TAG_WS;
                            gotTagName();
                            // ## changed
                            break;
                        case '/':
                            state = ST_EMPTY_TAG_SLASH;
                            gotTagName();
                            // ## changed
                            break;
                        case '>':
                            gotTagName();
                            // ## changed
                            gotTag();
                            break;
                        default:
                            tagname.write(Character.toLowerCase((char) c));
                        // ## changed
                    }
                    break;
                case ST_TAG_WS:
                    switch (c)
                    {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            break;
                        case '/':
                            state = ST_EMPTY_TAG_SLASH;
                            break;
                        case '>':
                            gotTag();
                            break;
                        case '?':
                        // NOTE: if !inXMLDecl we fall through to default case
                        default:
                            if (!isStartTag)
                            {
                                // bit of a hack this...
                                //errHandler.warning("Malformed tag: "+buf, sysID, _line, _column);
                                //err_continue("Malformed tag: "+buf);
                                toStart();
                                // ## changed
                                if (c == '<')
                                {
                                    gotPCDATA(true);
                                    keepPCData = false;
                                    state = ST_TAG_LT;
                                }
                                else
                                {
                                    // we get here e.g. if there's an end tag with attributes
                                    state = ST_PCDATA;
                                    pcData.reset();
                                }
                            }
                            else
                            {
                                // FIXME: this accepts way too many first chars for attr name
                                attrName.write(Character.toLowerCase((char) c));
                                state = ST_NAME;
                            }
                    }
                    break;
                case ST_EMPTY_TAG_SLASH:
                    if (c == '>')
                    {
                        //tagtype = TAG_EMPTY;
                        gotTag();
                        break;
                    }
                    else
                    {
                        // ERROR !? - can't throw Exception here - we go to next tag...
                        state = ST_PCDATA;
                        pcData.reset();
                    }
                    break;
                case ST_NAME:
                    switch (c)
                    {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            if (attrName.size() > 0)
                            {
                                state = ST_NAME_WS;
                            }
                            break;
                        case '>':
                            if (attrName.size() > 0)
                            {
                                gotAttr();
                            }
                            gotTag();
                            break;
                        case '=':
                            state = ST_EQ;
                            break;
                        default:
                            if (isCtlOrTspecial(c))
                            {
                                state = ST_PCDATA;
                                pcData.reset();
                            }
                            else
                            {
                                attrName.write(Character.toLowerCase((char) c));
                            }
                    }
                    break;
                case ST_NAME_WS:
                    // white-space between name and '='
                    switch (c)
                    {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            break;
                        case '=':
                            state = ST_EQ;
                            break;
                        case '>':
                            gotAttr();
                            gotTag();
                            break;
                        default:
                            if (isNameChar(c))
                            {
                                gotAttr();
                                attrName.write(Character.toLowerCase((char) c));
                                state = ST_TAG_WS;
                            }
                            else
                            {
                                state = ST_PCDATA;
                                pcData.reset();
                            }
                    }
                    break;
                case ST_EQ:
                    // white-space between '=' and value
                    switch (c)
                    {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            break;
                        case '"':
                            qchar = '"';
                            state = ST_VALUE_QUOTED;
                            break;
                        case '\'':
                            qchar = '\'';
                            state = ST_VALUE_QUOTED;
                            break;
                        default:
                            if (isCtlOrTspecial(c))
                            {
                                state = ST_PCDATA;
                                pcData.reset();
                            }
                            else
                            {
                                attrValue.write(c);
                                state = ST_VALUE;
                            }
                    }
                    break;
                case ST_VALUE:
                    switch (c)
                    {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            gotAttr();
                            state = ST_TAG_WS;
                            break;
                        case '>':
                            gotAttr();
                            gotTag();
                            break;
                        /*
                         *  case '/':     // FIXME: HTML knows things like <a href=a/b.html> !!
                         *  gotAttr();
                         *  state = ST_EMPTY_TAG_SLASH;
                         *  break;
                         */
                        default:
                            if (isValueBreaker(c))
                            {
                                state = ST_PCDATA;
                                pcData.reset();
                            }
                            else
                            {
                                attrValue.write(c);
                            }
                    }
                    break;
                case ST_VALUE_QUOTED:
                    if (c == qchar)
                    {
                        gotAttr();
                        state = ST_TAG_WS;
                    }
                    else
                    {
                        attrValue.write(c);
                    }
                    break;
                case ST_COMMENT:
                    // we've seen "...<!-" by now
                    try
                    {
                        if (c != '-')
                        {
                            //warning("Bad comment");
                            state = ST_PCDATA;
                            pcData.reset();
                            break;
                        }
                        // we're within comment - read till we see "--"
                        while (true)
                        {
                            while (read_ex() != '-')
                            {
                                ;
                            }
                            if (read_ex() == '-')
                            {
                                break;
                            }
                        }
                        // seen "--" - gotComment() reads past next '>'
                        gotComment();
                        //while (read_ex() != '>') ;
                        //state = ST_PCDATA;
                    }
                    catch (EmptyInputStream ex)
                    {
                        gotPCDATA(false);
                        keepPCData = false;
                        break;
                    }
                    break;
                case ST_IN_ANCHOR:
                     // we've seen <a href="...">. href is in linkValue. Read until
                     // the next end tag, at most 200 characters.
                     // (end tags are often ommited, i.e. <a ...>text</td>)
                     // regards other tags as text
                     // @todo: read until next </a> or a couple of other tags
                    try
                    {
                        short count = 0;
                        switch(c)
                        {
                            case '\t':
                            case '\n':
                            case '\r':
                                pcData.write(' ');
                                break;
                            default:
                                pcData.write(c);
                        }
                        while(count < MAX_PCDATA_LENGTH)
                        {
                            count++;
                            while(((c =read_ex()) != '<') && (count < MAX_PCDATA_LENGTH))
                            {
                                switch(c)
                                {
                                    case '\t':
                                    case '\n':
                                    case '\r':
                                        pcData.write(' ');
                                        break;
                                    default:
                                        pcData.write(c);
                                }
                            }
                            if(count >= MAX_PCDATA_LENGTH)
                            {
                                gotAnchor(false);
                                break;
                            }
                            else
                            {
                                pcData.write(c);
                                count++;
                                if((c=read_ex()) == '/')
                                {
                                    gotAnchor(true);
                                    isStartTag = false;
                                    state = ST_TAG_NAME;
                                    break;
                                }
                                else
                                {
                                    pcData.write(c);
                                }
                            }
                        }
                    }
                    catch(EmptyInputStream ex)
                    {

                    }
            }
        }

        // input stream ended - return rest, if any, as PCDATA
        if (buf.size() > 0)
        {
            gotPCDATA(false);
            keepPCData = false;
            buf.reset();
        }
    }


    // counts lines and columns - used in error reporting
    // a line can be a single \r or \n or it can be \r\n - we handle them all
    int cc;

    // last char read


    /**
     * Description of the Method
     *
     * @return                 Description of the Return Value
     * @exception IOException  Description of the Exception
     */
    public final int read()
        throws IOException
    {
        int c = in.read();
        if (c != -1)
        {
            buf.write(c);
        }

        return c;
    }


    /**
     * Description of the Method
     *
     * @return                      Description of the Return Value
     * @exception IOException       Description of the Exception
     * @exception EmptyInputStream  Description of the Exception
     */
    public final int read_ex()
        throws IOException, EmptyInputStream
    {
        int c = read();
        if (c == -1)
        {
            throw new EmptyInputStream();
        }
        return c;
    }


    // HTML allows <em>boolean</em> attributes - attributes without a
    // value, or rather an implicit value which is the same as the name.
    /**
     * Description of the Method
     *
     * @exception Exception  Description of the Exception
     */
    protected final void gotAttr()
        throws Exception
    {
        // gotTag has to be called first, setting waitForAtt = ATT_HREF or ATT_SRC
        if (!linkAttrFound)
        {
            char[] attName = attrName.getCharArray();
            int attLength = attrName.getLength();
            boolean gotcha = false;

            switch (attLength)
            {
                case 4:
                    if (attName[0] == 'h' && attName[1] == 'r' && attName[2] == 'e' && attName[3] == 'f')
                    {
                        gotcha = true;
                    }
                    break;
                case 3:
                    if (attName[0] == 's' && attName[1] == 'r' && attName[2] == 'c')
                    {
                        gotcha = true;
                    }
                    break;
            }
            if (gotcha)
            {
                linkValue = (rcgnzEntities ? entMngr.entityDecode(attrValue) :
                        attrValue).toString();
                linkAttrFound = true;
            }
            else
            {
                linkValue = "";
            }
        }
        attrName.reset();
        attrValue.reset();
        //attrs.put(nm, val);
    }

    protected void gotAnchor(boolean tooMuch)
    {
        String anchor;
        if(tooMuch)
        {
            anchor = pcData.toString().substring(0, pcData.getLength()-1);
        }
        else
        {
            anchor = pcData.toString();
        }
        linkHandler.handleLink(linkValue, anchor, false);
        toStart();
    }


    /**
     * Description of the Method
     */
    protected void gotTagName()
    {
        char[] tag = tagname.getCharArray();
        int tagLength = tagname.getLength();
        switch (tagLength)
        {
            case 1:
                // A
                if (isStartTag && tag[0] == 'a')
                {
                    linkTagType = LINKTYPE_A;
                    linkAttrType = ATTR_HREF;
                }
                break;
            // [case 3: // IMG]
            case 4:
                // BASE, AREA [, LINK]
                if(isStartTag)
                {
                    if (tag[0] == 'b' && tag[1] == 'a' && tag[2] == 's' && tag[3] == 'e')
                    {
                        linkTagType = LINKTYPE_BASE;
                        linkAttrType = ATTR_HREF;
                    }
                    else if (tag[0] == 'a' && tag[1] == 'r' && tag[2] == 'e' && tag[3] == 'a')
                    {
                        linkTagType = LINKTYPE_AREA;
                        linkAttrType = ATTR_HREF;
                    }
                }
                break;
            case 5:
                // FRAME
                if(isStartTag)
                {
                    if (tag[0] == 'f' && tag[1] == 'r' && tag[2] == 'a' && tag[3] == 'm' && tag[4] == 'e')
                    {
                        linkTagType = LINKTYPE_FRAME;
                        linkAttrType = ATTR_SRC;
                    }
                    else if (tag[0] == 't' && tag[1] == 'i' && tag[2] == 't' && tag[3] == 'l' && tag[4] == 'e')
                    {
                        isInTitleTag = true;
                        keepPCData = true;
                    }
                }
            default:
        }
    }


    /**
     * Description of the Method
     *
     * @exception Exception  Description of the Exception
     */
    protected void gotTag()
        throws Exception
    {
        if (linkAttrFound && isStartTag)
        {
            switch (linkTagType)
            {
                case LINKTYPE_AREA:
                    //System.out.println("got link " + linkValue);
                    linkHandler.handleLink(linkValue, /*@todo altText*/ null, false);
                    break;
                case LINKTYPE_FRAME:
                    //System.out.println("got link " + linkValue);
                    linkHandler.handleLink(linkValue, null, true);
                    break;
                case LINKTYPE_BASE:
                    linkHandler.handleBase(linkValue);
                    break;
                case LINKTYPE_A:
                    state = ST_IN_ANCHOR;
                    return;
            }
        }
        toStart();
    }


    /**
     * Description of the Method
     *
     * @param attrs  Description of the Parameter
     *
    public final void keysToLowerCase(SAXAttributeMap attrs)
    {
        for (int i = 0; i < attrs.n; i++)
        {
            attrs.keys[i] = attrs.keys[i].toLowerCase();
            if (atomize)
            {
                attrs.keys[i] = Atom.getAtom(attrs.keys[i]);
            }
        }
    }
    */

    // toomuch true iff we read a '<' of the next token
    /**
     * Description of the Method
     *
     * @param toomuch        Description of the Parameter
     * @exception Exception  Description of the Exception
     */
    protected void gotPCDATA(boolean toomuch)
        throws Exception
    {
        if(isInTitleTag)
        {
            linkHandler.handleTitle(pcData.toString());
            isInTitleTag = false;
        }
        else if(isInAnchorTag)
        {
            linkHandler.handleLink(this.linkValue, pcData.toString(), false);
            isInAnchorTag = false;
        }

        // ignore it
        toStart();
    }


    /*
     *  noChildren = false;
     *  if (toomuch) {
     *  buf.setLength(buf.size() - 1);
     *  }
     *  SimpleCharArrayWriter buf1 = rcgnzEntities ? entMngr.entityDecode(buf) : buf;
     *  docHandler.characters(buf1.getCharArray(), 0, buf1.size());
     *  /handler.gotText(getBuffer());
     *  toStart();
     *  if (toomuch) {
     *  buf.write('<');
     *  column--;
     *  }
     *  }
     */
    // XXX: should pass the comment on as docHandler.ignorable() ??
    /**
     * Description of the Method
     *
     * @exception IOException       Description of the Exception
     * @exception EmptyInputStream  Description of the Exception
     */
    protected void gotComment()
        throws IOException, EmptyInputStream
    {
        //toStart();  // so an unexpected EOF causes rest to be returned as PCDATA
        while (read_ex() != '>')
        {
            ;
        }
        toStart();
    }


    // Processing Instruction
    /**
     * Description of the Method
     *
     * @exception Exception  Description of the Exception
     */
    protected void parsePI()
        throws Exception
    {
        // ignore this

        /*
         *  int i;
         *  String target;
         *  noChildren = false;
         *  inXMLDecl = false;
         *  i = buf.size();
         *  try {
         *  while (!isWS(read_ex())) ;
         *  target = buf.toString();
         *  target = target.substring(i, target.length() - 1);
         *  if ("XML".equals(target)) {
         *  inXMLDecl = true;
         *  state = ST_TAG_WS;
         *  return;
         *  }
         *  while (isWS(read_ex())) ;
         *  i = buf.size() - 1;
         *  while (true) {
         *  while (read_ex() != '?') ;
         *  if (read_ex() == '>') {
         *  String s = buf.toString();
         *  docHandler.processingInstruction(
         *  Atom.getAtom(target), s.substring(i, s.length()-2));
         *  /handler.gotPI(Atom.getAtom(target),
         *  /              s.substring(i, s.length()-2));
         *  break;
         *  }
         *  }
         *  } catch (EmptyInputStream ex) {
         *  gotPCDATA(false);
         *  errHandler.warning("EOF while parsing PI", sysID, _line, _column);
         *  /err_continue("EOF while parsing PI");
         *  }
         */
        toStart();
    }


    // CDATA section
    // XXX: should contents be amalgamated with surrounding PCDATA?
    /**
     * Description of the Method
     *
     * @exception Exception  Description of the Exception
     */
    protected void parseCDATA()
        throws Exception
    {
        // we've seen "<![" by now
        try
        {
            if (read_ex() == 'C' && read_ex() == 'D' && read_ex() == 'A' &&
                    read_ex() == 'T' && read_ex() == 'A' && read_ex() == '[')
            {
                int i1 = buf.size();
                while (read_ex() != ']' ||
                        read_ex() != ']' ||
                        read_ex() != '>')
                {
                    ;
                }
                // docHandler.characters(buf.getCharArray(), i1, buf.size()-3-i1);
            }
            else
            {
                warning("Bad CDATA markup");
                state = ST_PCDATA;
                pcData.reset();
            }
        }
        catch (EmptyInputStream ex)
        {
            warning("EOF while parsing CDATA section");
            //gotPCDATA(false);
        }
        toStart();
    }


    /**
     * Gets the wS attribute of the Tokenizer object
     *
     * @param c  Description of the Parameter
     * @return   The wS value
     */
    public boolean isWS(int c)
    {
        switch (c)
        {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                return true;
            default:
                return false;
        }
    }


    /**
     * Gets the valueBreaker attribute of the Tokenizer class
     *
     * @param c  Description of the Parameter
     * @return   The valueBreaker value
     */
    public final static boolean isValueBreaker(int c)
    {
        switch (c)
        {
            // control characters (0-31 and 127):
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
            case 17:
            case 18:
            case 19:
            case 20:
            case 21:
            case 22:
            case 23:
            case 24:
            case 25:
            case 26:
            case 27:
            case 28:
            case 29:
            case 30:
            case 31:
            case 127:

            // tspecials:
            case '>':
            case ' ':
                return true;
            default:
                return false;
        }
    }


    /**
     * Returns true if c is either an ascii control character or a tspecial
     * according to the HTTP specification.
     *
     * @param c  Description of the Parameter
     * @return   The ctlOrTspecial value
     */
    //   private static final boolean[] isCtlOrTSpecial = new boolean[]
//     {
//        /* 0 */     true , true , true , true , true , true , true , true , true , true , true , true , true , true ,
//        /* 14 */    true , true , true , true , true , true , true , true , true , true , true , true , true , true ,
//        /* 28 */    true , true , true , true , true , false, true , false, false, false, false, false, true , true ,
//        /* 42 */    false, false, true , false, false, true , false, false, false, false, false, false, false, false,
//        /* 56 */    false, false, /*FIX: / no control char: true*/ false, true , true , true , true , true , true , false, false, false, false, false,
//        /* 70 */    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 84 */    false, false, false, false, false, false, false, true , true , true , false, false, false, false,
//        /* 98 */    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 112 */   false, false, false, false, false, false, false, false, false, false, false, true , false, true ,
//        /* 126 */   false, true , false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 140 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 154 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 168 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 182 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 196 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 210 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 224 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 238 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 252 */   false, false, false, false
//    };

    public final static boolean isCtlOrTspecial(int c)
    {
        switch (c)
        {
            // control characters (0-31 and 127):
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
            case 17:
            case 18:
            case 19:
            case 20:
            case 21:
            case 22:
            case 23:
            case 24:
            case 25:
            case 26:
            case 27:
            case 28:
            case 29:
            case 30:
            case 31:
            case 127:

            // tspecials:
            case '(':
            case ')':
            case '<':
            case '>':
            case '@':
            case ',':
            case ';':
            case ':':
            case '\\':
            case '"':
            /*
             *  case '/':
             */
            case '[':
            case ']':
            case '?':
            case '=':
            case '{':
            case '}':
            case ' ':
                // case '\t':
                return true;
            default:
                return false;
        }
    }


    /*
     *  public static void main(String[])
     *  {
     *  System.out.println("private static final boolean[] isCtlOrTSpecial = \n{");  // bzw. isNameChar
     *  for(int i=0; i<256; i++)
     *  {
     *  if(i>0)
     *  System.out.print(", ");
     *  if(i % 14 == 0)
     *  {
     *  System.out.print("\n/* " + i + " *" + "/   ");
     *  }
     *  if(Tokenizer.isCtlOrTspecial(i))  // bzw. isNameChar(i)
     *  {
     *  System.out.print("true ");
     *  }
     *  else
     *  {
     *  System.out.print("false");
     *  }
     *  }
     *  System.out.print("};\n\n");
     *  }
     */
//    public static final boolean isCtlOrTspecial(int c)
//    {
//        return (c < 256 ? isCtlOrTSpecial[c] : false);
//    }
//
//    private static final boolean[] isNameChar =
//    {
//        /* 0 */     false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 14 */    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 28 */    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 42 */    false, false, false, true , true , false, true , true , true , true , true , true , true , true ,
//        /* 56 */    true , true , false, false, false, false, false, false, false, true , true , true , true , true ,
//        /* 70 */    true , true , true , true , true , true , true , true , true , true , true , true , true , true ,
//        /* 84 */    true , true , true , true , true , true , true , false, false, false, false, true , false, true ,
//        /* 98 */    true , true , true , true , true , true , true , true , true , true , true , true , true , true ,
//        /* 112 */   true , true , true , true , true , true , true , true , true , true , true , false, false, false,
//        /* 126 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 140 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 154 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 168 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 182 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 196 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 210 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 224 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 238 */   false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//        /* 252 */   false, false, false, false
//    };
//    public static final boolean isNameChar(int c)
//    {
//        return (c < 256 ? isNameChar[c] : false);
//    }
//
    /*
     *  / I don't think this is a very standard definition of what can
     *  / go into tag and attribute names.
     */
    /**
     * Gets the nameChar attribute of the Tokenizer class
     *
     * @param c  Description of the Parameter
     * @return   The nameChar value
     */
    public final static boolean isNameChar(int c)
    {
        return ('a' <= c && c <= 'z') ||
                ('A' <= c && c <= 'Z') ||
                ('0' <= c && c <= '9') ||
                c == '.' || c == '-' || c == '_';
    }



    /**
     * Description of the Method
     *
     * @param s              Description of the Parameter
     * @exception Exception  Description of the Exception
     */
    protected final void warning(String s)
        throws Exception
    {
        //errHandler.warning(s, sysID, _line, _column);
    }


    /**
     * Description of the Method
     *
     * @param s              Description of the Parameter
     * @exception Exception  Description of the Exception
     */
    protected final void fatal(String s)
        throws Exception
    {
        //errHandler.fatal(s, sysID, _line, _column);
    }



    /**
     * The main program for the Tokenizer class
     *
     * @param argv  The command line arguments
     */
    public static void main(String[] argv)
    {
        Tokenizer tok = new Tokenizer();
        tok.setLinkHandler(
            new LinkHandler()
            {
                int nr = 0;


                public void handleLink(String link, String anchor, boolean isFrame)
                {
                    System.out.println("found link " + (++nr) + ": " + link + "; Text: " + anchor);
                }
                public void handleTitle(String title)
                {
                    System.out.println("found title " + (++nr) + ": " + title);
                }


                public void handleBase(String link)
                {
                    System.out.println("found base " + (++nr) + ": " + link);
                }
            });
        try
        {
            tok.parse(new FileReader("C:\\witest.htm"));
            /*
             *  "<frame src=\\"link1\"> </head>" +
             *  "This is some Text\n" +
             *  "<a name=_sometest href='link2'>and this is... the link</a>" +
             *  "<table width=234><base href=\"'link3'\">"));
             */
        }
        catch (Exception e)
        {
            System.out.println("Caught Exception: " + e.getClass().getName());
            e.printStackTrace();
        }
    }
}

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   29. Dezember 2001
 */
class EmptyInputStream extends Exception
{


    /**
     * Constructor for the EmptyInputStream object
     */
    EmptyInputStream() { }

}

