/*
 * $Id$
 *
 * Copyright 1997 Hewlett-Packard Company
 *
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

/*
 * FIXME:
 *   - use java.io.Reader and Unicode chars...
 *   - recognize PIs and CDATA
 *   - recognize PEs and CEs (optionally)
 *   - Do NOT map element and attr names to lower (or upper) case
 */

package hplb.xml;

import hplb.org.xml.sax.*;
import java.util.Dictionary;
import java.util.Hashtable;
import java.io.*;
import hplb.misc.ByteArray;
import java.net.URL;

/**
 * This is a hand-written lexical analyzer for XML/HTML Markup.
 * The parser is simple, fast and quite robust.
 * Element and attribute names are mapped to lower case.
 * Comments are returned as (part of) PCDATA tokens.
 * Markup elements within comments is not recognized as markup.
 *
 * @author      Anders Kristensen
 */
public class Tokenizer implements hplb.org.xml.sax.Parser {

    /** The value of boolean attributes is this string. */
    public static final String BOOLATTR = Atom.getAtom("BOOLATTR");

    // FSM states:
    static final int ST_START           = 1;
    static final int ST_TAG_LT          = 3;
    static final int ST_TAG_NAME        = 4;
    static final int ST_TAG_WS          = 5;
    static final int ST_EMPTY_TAG_SLASH = 6;
    static final int ST_NAME            = 7;
    static final int ST_NAME_WS         = 8;
    static final int ST_EQ              = 9;
    static final int ST_VALUE           = 10;
    static final int ST_VALUE_QUOTED    = 11;
    static final int ST_PCDATA          = 21;
    static final int ST_COMMENT         = 22;

    HandlerBase    dfltHandler = new HandlerBase();
    EntityHandler   entHandler = dfltHandler;
    DocumentHandler docHandler = dfltHandler;
    ErrorHandler    errHandler = dfltHandler;
    SAXAttributeMap attrs = new SAXAttributeMap();
    String sysID;

    protected Hashtable noCaseElms;
    public boolean rcgnzWS       = true;   // is white space chars recognized as PCDATA
                                           // even when preceeding tags?
    public boolean rcgnzEntities = true;
    public boolean rcgnzCDATA    = true;
    public boolean rcgnzComments = true;   //
    public boolean atomize       = false;  // make element and attr names atoms

    CharBuffer buf       = new CharBuffer();
    boolean isStartTag   = true;
    /**
     * Signals whether a non-empty element has any children. If not we
     * must generate an artificial empty-string child [characters(buf, 0, 0)].
     */
    boolean noChildren;
    CharBuffer tagname   = new CharBuffer();
    CharBuffer attrName  = new CharBuffer();
    CharBuffer attrValue = new CharBuffer();
    Reader in;

    public final EntityManager entMngr = new EntityManager(this);
    protected int state = ST_START;
    protected int _line = 1;
    protected int _column = 0;
    public int line;          // can be used in Handler callbacks
    public int column;        // can be used in Handler callbacks
    protected int qchar;      // <'> or <"> when parsing quoted attr values
    // we recognize attribute name-value pairs for XML PI by setting
    // the inXMLDecl flag and going to state ST_TAG_WS
    boolean inXMLDecl = false;  // see

    public Tokenizer() {
        pos();
    }

    public void setEntityHandler(EntityHandler handler) {
        entHandler = handler;
    }

    public void setDocumentHandler(DocumentHandler handler) {
        docHandler = handler;
    }

    public void setErrorHandler(ErrorHandler handler) {
        errHandler = handler;
    }

    public void parse(String publicID, String sysID) throws Exception {
        this.sysID = sysID;
        parse(new URL(sysID).openStream());
    }

    public void parse(InputStream in) throws Exception
	{
		parse(new InputStreamReader(in));
    }

	public void parse(Reader in) throws Exception
	{
        this.in = in;
        docHandler.startDocument();
        tokenize();
        docHandler.endDocument();
	}

    // invoked to remember current position
    protected void pos() {
        line = _line;
        column = _column;
    }

    public void ignoreCase(String elementName) {
        if (noCaseElms == null) noCaseElms = new Hashtable();
        noCaseElms.put(elementName.toLowerCase(), elementName);
    }

    public void rcgnzWS(boolean b) {
        rcgnzWS = b;
    }

    // invoked after doing any Handler callback - resets state
    protected void toStart() {
        state = ST_START;
        buf.reset();
        tagname.reset();
        attrName.reset();
        attrValue.reset();
        attrs.clear();
        isStartTag = true;  // until proven wrong
        pos();
    }

  public void tokenize() throws Exception {
    int c;

    while ((c = read()) != -1) {
      switch (state) {
        case ST_START:
          switch (c) {
            case '<':
              state = ST_TAG_LT;
              isStartTag = true;  // until proven wrong
              tagname.reset();
              break;
            case ' ': case '\t': case '\r': case '\n':
              if (!rcgnzWS) break;
              // else fall through
            default:
              state = ST_PCDATA;
          }
          break;

        case ST_PCDATA:
          if (c == '<') {
            gotPCDATA(true);
            state = ST_TAG_LT;
          }
          break;

        case ST_TAG_LT:
          switch (c) {
            case '/':
              isStartTag = false;
              state = ST_TAG_NAME;
              break;
            case '!':
              c = read();
              if ((c == '-' && !rcgnzComments) || (c == '[' && !rcgnzCDATA)) {
                state = ST_PCDATA;
                break;
              }
              if (c == '-') state = ST_COMMENT;
              else if (c == '[') parseCDATA();
              else {
                // FIXME: shouldn't be delivered as PCDATA
                warning("Bad markup " + buf);
                state = ST_PCDATA;
              }
              break;
            case '?':
              parsePI();
              break;
            case ' ': case '\t': case '\r': case '\n':
              state = ST_TAG_WS;
              break;
            default:
              tagname.write(c);
              state = ST_TAG_NAME;
          }
          break;

        case ST_TAG_NAME:
          switch (c) {
            case ' ': case '\t': case '\r': case '\n':
              state = ST_TAG_WS;
              break;
            case '/': state = ST_EMPTY_TAG_SLASH; break;
            case '>': gotTag(false); break;
            default:  tagname.write(c);
          }
          break;

        case ST_TAG_WS:
          switch (c) {
            case ' ': case '\t': case '\r': case '\n': break;
            case '/': state = ST_EMPTY_TAG_SLASH; break;
            case '>': gotTag(false); break;
            case '?':
              if (inXMLDecl) {
                if ((c = read()) != '>') {
                errHandler.warning("XML PI not terminated properly",
                                   sysID, _line, _column);
                  //err_continue("XML PI not terminated properly");
                }
                //handler.gotXMLDecl(attrs);  // FIXME(?)
                toStart();
                break;
              }
              // NOTE: if !inXMLDecl we fall through to default case
            default:
              if (!isStartTag) {
                // bit of a hack this...
                errHandler.warning("Malformed tag: "+buf, sysID, _line, _column);
                //err_continue("Malformed tag: "+buf);
                if (c == '<') {
                    gotPCDATA(true);
                    state = ST_TAG_LT;
                } else {
                    // we get here e.g. if there's an end tag with attributes
                    state = ST_PCDATA;
                }
              } else {
                // FIXME: this accepts way too many first chars for attr name
                attrName.write(c);
                state = ST_NAME;
              }
          }
          break;

        case ST_EMPTY_TAG_SLASH:
          if (c == '>') {
            //tagtype = TAG_EMPTY;
            gotTag(true);
            break;
          } else {
            // ERROR !? - can't throw Exception here - we go to next tag...
            state = ST_PCDATA;
          }
          break;

        case ST_NAME:
          switch (c) {
            case ' ': case '\t': case '\r': case '\n':
              if (attrName.size() > 0) {
                state = ST_NAME_WS;
              }
              break;
            case '>':
              if (attrName.size() > 0) gotAttr(true);
              gotTag(false);
              break;
            case '=':
              state = ST_EQ;
              break;
            default:
              if (isCtlOrTspecial(c)) {
                state = ST_PCDATA;
              } else {
                attrName.write(c);
              }
          }
          break;

        case ST_NAME_WS:   // white-space between name and '='
          switch (c) {
            case ' ': case '\t': case '\r': case '\n': break;
            case '=': state = ST_EQ; break;
            case '>': gotAttr(true); gotTag(false); break;
            default:
              if (isNameChar(c)) {
                gotAttr(true);
                attrName.write(c);
                state = ST_TAG_WS;
              } else {
                state = ST_PCDATA;
              }
          }
          break;

        case ST_EQ:        // white-space between '=' and value
          switch (c) {
            case ' ': case '\t': case '\r': case '\n': break;
            case '"':  qchar = '"';  state = ST_VALUE_QUOTED; break;
            case '\'': qchar = '\''; state = ST_VALUE_QUOTED; break;
            default:
              if (isCtlOrTspecial(c)) {
                state = ST_PCDATA;
              } else {
                attrValue.write(c);
                state = ST_VALUE;
              }
          }
          break;

        case ST_VALUE:
          switch (c) {
            case ' ': case '\t': case '\r': case '\n':
              gotAttr(false);
              state = ST_TAG_WS;
              break;
            case '>':
              gotAttr(false);
              gotTag(false);
              break;
            case '/':
              gotAttr(false);
              state = ST_EMPTY_TAG_SLASH;
              break;
            default:
              if (isCtlOrTspecial(c)) {
                state = ST_PCDATA;
              } else {
                attrValue.write(c);
              }
          }
          break;

        case ST_VALUE_QUOTED:
          if (c == qchar) {
            gotAttr(false);
            state = ST_TAG_WS;
          } else {
            attrValue.write(c);
          }
          break;

        case ST_COMMENT:
          // we've seen "...<!-" by now
          try {
            if (c != '-') {
              warning("Bad comment");
              state = ST_PCDATA;
              break;
            }
            // we're within comment - read till we see "--"
            while (true) {
              while (read_ex() != '-') ;
              if (read_ex() == '-') break;
            }
            // seen "--" - gotComment() reads past next '>'
            gotComment();
            //while (read_ex() != '>') ;
            //state = ST_PCDATA;
          } catch (EmptyInputStream ex) {
            gotPCDATA(false);
            break;
          }
      }
    }
    /* TODO: catch EmptyInputStream exception only here!
    } catch (EmptyInputStream ex) {
        err_continue("EOF while parsing " + token[state]);
    }
    */

    // input stream ended - return rest, if any, as PCDATA
    if (buf.size() > 0) {
        gotPCDATA(false);
        buf.reset();
        }
    }

    // counts lines and columns - used in error reporting
    // a line can be a single \r or \n or it can be \r\n - we handle them all
    int cc; // last char read
    public final int read() throws IOException {
        int c = in.read();
        if (c != -1) {
            buf.write(c);

            switch (c) {
                case '\r': _line++; _column = 0; break;
                case '\n':
                    if (cc != '\r') _line++;
                    _column = 0;
                    break;
                default:
                    _column++;
            }
            cc = c;
        }
        return c;
    }

    public final int read_ex() throws IOException, EmptyInputStream {
        int c = read();
        if (c == -1) throw new EmptyInputStream();
        return c;
    }

    // HTML allows <em>boolean</em> attributes - attributes without a
    // value, or rather an implicit value which is the same as the name.
    protected final void gotAttr(boolean isBoolean) throws Exception {
        String nm = attrName.toString();
        if (atomize) nm = Atom.getAtom(nm);
        String val = isBoolean ? BOOLATTR :
                        (rcgnzEntities ? entMngr.entityDecode(attrValue) :
                            attrValue).toString();
        attrName.reset();
        attrValue.reset();
        attrs.put(nm, val);
    }

    protected void gotTag(boolean isEmpty) throws Exception {
        String nm = tagname.toString();
        String nm_lc = nm.toLowerCase();
        if (noCaseElms != null && noCaseElms.get(nm_lc) != null) {
            nm = nm_lc;
            keysToLowerCase(attrs);
        }
        if (atomize) nm = Atom.getAtom(nm);
        if (isStartTag) {
            docHandler.startElement(nm, attrs);
            //handler.gotSTag(nm, isEmpty, attrs, getBuffer());
            if (isEmpty) docHandler.endElement(nm);
            noChildren = !isEmpty;
        } else {
            if (noChildren) {
                docHandler.characters(buf.getCharArray(), 0, 0);
                noChildren = false;
            }
            docHandler.endElement(nm);
            //handler.gotETag(nm, getBuffer());
        }
        toStart();
    }

    public final void keysToLowerCase(SAXAttributeMap attrs) {
        for (int i = 0; i < attrs.n; i++) {
            attrs.keys[i] = attrs.keys[i].toLowerCase();
            if (atomize) attrs.keys[i] = Atom.getAtom(attrs.keys[i]);
        }
    }

    // toomuch true iff we read a '<' of the next token
    protected void gotPCDATA(boolean toomuch) throws Exception {
        noChildren = false;
        if (toomuch) {
            buf.setLength(buf.size() - 1);
        }
        CharBuffer buf1 = rcgnzEntities ? entMngr.entityDecode(buf) : buf;
        docHandler.characters(buf1.getCharArray(), 0, buf1.size());
        //handler.gotText(getBuffer());
        toStart();
        if (toomuch) {
            buf.write('<');
            column--;
        }
    }

    // XXX: should pass the comment on as docHandler.ignorable() ??
    protected void gotComment() throws IOException, EmptyInputStream {
        //toStart();  // so an unexpected EOF causes rest to be returned as PCDATA
        while (read_ex() != '>') ;
        toStart();
    }

    // Processing Instruction
    protected void parsePI() throws Exception {
        int i;
        String target;

        noChildren = false;
        inXMLDecl = false;
        i = buf.size();
        try {
        while (!isWS(read_ex())) ;
        target = buf.toString();
        target = target.substring(i, target.length() - 1);

        if ("XML".equals(target)) {
            inXMLDecl = true;
            state = ST_TAG_WS;
            return;
        }

        while (isWS(read_ex())) ;
        i = buf.size() - 1;
        while (true) {
            while (read_ex() != '?') ;
            if (read_ex() == '>') {
                String s = buf.toString();
                docHandler.processingInstruction(
                        Atom.getAtom(target), s.substring(i, s.length()-2));
                //handler.gotPI(Atom.getAtom(target),
                //              s.substring(i, s.length()-2));
                break;
            }
        }
        } catch (EmptyInputStream ex) {
            gotPCDATA(false);
            errHandler.warning("EOF while parsing PI", sysID, _line, _column);
            //err_continue("EOF while parsing PI");
        }
        toStart();
    }

    // CDATA section
    // XXX: should contents be amalgamated with surrounding PCDATA?
    protected void parseCDATA() throws Exception {
        // we've seen "<![" by now
        try {
            if (read_ex() == 'C' && read_ex() == 'D' && read_ex() == 'A' &&
                read_ex() == 'T' && read_ex() == 'A' && read_ex() == '[') {
                int i1 = buf.size();
                while (read_ex() != ']' ||
                       read_ex() != ']' ||
                       read_ex() != '>') ;
                docHandler.characters(buf.getCharArray(), i1, buf.size()-3-i1);
            } else {
                warning("Bad CDATA markup");
                state = ST_PCDATA;
            }
        } catch (EmptyInputStream ex) {
            warning("EOF while parsing CDATA section");
            gotPCDATA(false);
        }
        toStart();
    }

    public boolean isWS(int c) {
        switch (c) {
            case ' ': case '\t': case '\r': case '\n': return true;
            default: return false;
        }
    }

    /**
     * Returns true if c is either an ascii control character or
     * a tspecial according to the HTTP specification.
     */
  //   private static final boolean[] isCtlOrTSpecial = new boolean[]
//     {
//        /* 0 */     true , true , true , true , true , true , true , true , true , true , true , true , true , true ,
//        /* 14 */    true , true , true , true , true , true , true , true , true , true , true , true , true , true ,
//        /* 28 */    true , true , true , true , true , false, true , false, false, false, false, false, true , true ,
//        /* 42 */    false, false, true , false, false, true , false, false, false, false, false, false, false, false,
//        /* 56 */    false, false, true , true , true , true , true , true , true , false, false, false, false, false,
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

    public static final boolean isCtlOrTspecial(int c) {
        switch (c) {
          // control characters (0-31 and 127):
          case  0: case  1: case  2: case  3: case  4: case  5:
          case  6: case  7: case  8: case  9: case 10: case 11:
          case 12: case 13: case 14: case 15: case 16: case 17:
          case 18: case 19: case 20: case 21: case 22: case 23:
          case 24: case 25: case 26: case 27: case 28: case 29:
          case 30: case 31: case 127:

          // tspecials:
          case '(': case ')': case '<': case '>': case '@':
          case ',': case ';': case ':': case '\\': case '"':
          case '/': case '[': case ']': case '?': case '=':
          case '{': case '}': case ' ': // case '\t':
            return true;

          default:
            return false;
        }
    }

/*    public static void main(String[])
    {
    System.out.println("private static final boolean[] isCtlOrTSpecial = \n{");  // bzw. isNameChar
        for(int i=0; i<256; i++)
        {
            if(i>0)
                System.out.print(", ");
            if(i % 14 == 0)
            {
                System.out.print("\n/* " + i + " *" + "/   ");
            }
            if(Tokenizer.isCtlOrTspecial(i))  // bzw. isNameChar(i)
            {
                System.out.print("true ");
            }
            else
            {
                System.out.print("false");
            }


        }
        System.out.print("};\n\n");
    }
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
    // I don't think this is a very standard definition of what can
    // go into tag and attribute names.*/
    public static final boolean isNameChar(int c) {
        return ('a' <= c && c <= 'z') ||
               ('A' <= c && c <= 'Z') ||
               ('0' <= c && c <= '9') ||
               c == '.' || c == '-' || c == '_';
    }



    protected final void warning(String s) throws Exception {
        errHandler.warning(s, sysID, _line, _column);
    }

    protected final void fatal(String s) throws Exception {
        errHandler.fatal(s, sysID, _line, _column);
    }
}

class EmptyInputStream extends Exception {
    EmptyInputStream() {}
}
