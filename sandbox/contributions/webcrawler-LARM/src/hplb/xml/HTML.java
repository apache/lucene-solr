/*
 * $Id$
 * 
 * Copyright 1997 Hewlett-Packard Company
 * 
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.xml;

/**
 * Parser customizations for correctly parsing HTML.
 * Defines a set of empty elements (&lt;hr&gt;, &lt;br&gt;, etc.)
 * and for some elements it defines which other start tags
 * implicitly ends them. As an example, an &lt;li&gt; element within
 * a &lt;ul&gt; list is terminated by either a &lt;/ul&gt; end tag
 * or another &lt;li&gt; start tag.
 *
 * @author  Anders Kristensen
 */
public class HTML {
    public static String A          = a("a");//
    public static String ACRONYM    = a("acronym");//
    public static String ADDRESS    = a("address");
    public static String APPLET     = a("applet");//
    public static String AREA       = a("area");
    public static String B          = a("b");//
    public static String BASE       = a("base");
    public static String BASEFONT   = a("basefont");//
    public static String BDO        = a("bdo");//
    public static String BIG        = a("big");//
    public static String BLOCKQUOTE = a("blockquote");
    public static String BODY       = a("body");//
    public static String BR         = a("br");
    public static String BUTTON     = a("button");//
    public static String CAPTION    = a("caption");//
    public static String CENTER     = a("center");
    public static String CITE       = a("cite");//
    public static String CODE       = a("code");//
    public static String COL        = a("col");
    public static String COLGROUP   = a("colgroup");//
    public static String DD         = a("dd");
    public static String DEL        = a("del");//
    public static String DFN        = a("dfn");//
    public static String DIR        = a("dir");
    public static String DIV        = a("div");
    public static String DL         = a("dl");
    public static String DT         = a("dt");
    public static String EM         = a("em");//
    public static String FIELDSET   = a("fieldset");
    public static String FONT       = a("font");//
    public static String FORM       = a("form");
    public static String FRAME      = a("frame");
    public static String FRAMESET   = a("frameset");//
    public static String H1         = a("h1");
    public static String H2         = a("h2");
    public static String H3         = a("h3");
    public static String H4         = a("h4");
    public static String H5         = a("h5");
    public static String H6         = a("h6");
    public static String HEAD       = a("head");
    public static String HR         = a("hr");
    public static String HTML       = a("html");
    public static String I          = a("i");//
    public static String IFRAME     = a("iframe");//
    public static String IMG        = a("img");
    public static String INPUT      = a("input");
    public static String INS        = a("ins");//
    public static String ISINDEX    = a("isindex");//
    public static String KBD        = a("kbd");//
    public static String LABEL      = a("label");//
    public static String LEGEND     = a("legend");//
    public static String LI         = a("li");
    public static String LINK       = a("link");
    public static String MAP        = a("map");//
    public static String MENU       = a("menu");
    public static String META       = a("meta");
    public static String NOFRAMES   = a("noframes");//
    public static String NOSCRIPT   = a("noscript");
    public static String OBJECT     = a("object");//
    public static String OL         = a("ol");
    public static String OPTION     = a("option");//
    public static String P          = a("p");
    public static String PARAM      = a("param");
    public static String PRE        = a("pre");
    public static String Q          = a("q");//
    public static String S          = a("s");//
    public static String SAMP       = a("samp");//
    public static String SCRIPT     = a("script");//
    public static String SELECT     = a("select");//
    public static String SMALL      = a("small");//
    public static String SPAN       = a("span");//
    public static String STRIKE     = a("strike");//
    public static String STRONG     = a("strong");//
    public static String STYLE      = a("style");//
    public static String SUB        = a("sub");//
    public static String SUP        = a("sup");//
    public static String TABLE      = a("table");
    public static String TBODY      = a("tbody");//
    public static String TD         = a("td");//
    public static String TEXTAREA   = a("textarea");//
    public static String TFOOT      = a("tfoot");//
    public static String TH         = a("th");//
    public static String THEAD      = a("thead");//
    public static String TITLE      = a("title");//
    public static String TR         = a("tr");
    public static String TT         = a("tt");//
    public static String U          = a("u");//
    public static String UL         = a("ul");
    public static String VAR        = a("var");//
    
    private static String a(String s) { return Atom.getAtom(s); }
    
    /** The full set of HTML4.0 element names. */
    public static final String[] elements = {
        A, ACRONYM, ADDRESS, APPLET, AREA , B, BASE, BASEFONT, BDO, BIG,
        BLOCKQUOTE, BODY, BR, BUTTON, CAPTION, CENTER, CITE, CODE, COL,
        COLGROUP, DD, DEL, DFN, DIR, DIV, DL, DT, EM, FIELDSET, FONT, FORM,
        FRAME, FRAMESET, H1, H2, H3, H4, H5, H6, HEAD, HR, HTML, I, IFRAME,
        IMG, INPUT, INS, ISINDEX, KBD, LABEL, LEGEND, LI, LINK, MAP, MENU,
        META, NOFRAMES, NOSCRIPT, OBJECT, OL, OPTION, P, PARAM, PRE, Q, S,
        SAMP, SCRIPT, SELECT, SMALL, SPAN, STRIKE, STRONG, STYLE, SUB, SUP,
        TABLE, TBODY, TD, TEXTAREA, TFOOT, TH, THEAD, TITLE, TR, TT, U, UL, VAR
    };
    
    // FIXME: the parser kindof supports optional end tags but not
    //        at all optional start tags (eg <html>, <head>)
    // FIXME: add support for HTML entities not in HTML (lots of those)

    // FIXME: this list probably not complete!!!
    /** Empty elements in HTML4.0: <em>br</em>, <em>img</em>, etc. */
    public static final String[] emptyElms = {
        AREA, BASE, BR, COL, FRAME, HR, IMG, LINK, META, PARAM };

    public static final String[] li_terminators = { LI };
    public static final String[] dt_terminators = { DT, DD };
    public static final String[] dd_terminators = dt_terminators;
    // <head> terminators: <body> and just about everything else

    /** Block-level HTML4.0 elements. */
    public static final String[] block_level = {
        ADDRESS, BLOCKQUOTE, CENTER, DIR, DIV, DL, FIELDSET, FORM,
        H1, H2, H3, H4, H5, H6, HR, MENU, NOSCRIPT, OL, P, PRE, TABLE, UL };

    // The P element can contain any *inline* markup - hence it is
    // terminated by any *blocklevel* markup (incl. other P elements):
    public static final String[] p_terminators = block_level;

    // elements which cannot contain PCDATA don't care about whitespace
    // FIXME: ignore_ws probably not complete  [don't include empty elements]
    public static final String[] ignore_ws = {
        HEAD, HTML, OL, MENU, TABLE, TR , UL };
    
    public static void applyHacks(Tokenizer tok) {
        for (int i = 0; i < elements.length; i++) {
            tok.ignoreCase(elements[i]);
        }
        
        EntityManager entMngr = tok.entMngr;
        
        // standard SGML entities
        entMngr.defTextEntity("amp", "&");    // ampersand
        entMngr.defTextEntity("gt", ">");     // greater than
        entMngr.defTextEntity("lt", "<");     // less than
        entMngr.defTextEntity("quot", "\"");  // double quote

        // PUBLIC ISO 8879-1986//    entities Added Latin 1//EN//HTML
        entMngr.defTextEntity("AElig",  "\u00c6");   // capital AE diphthong (ligature)
        entMngr.defTextEntity("Aacute", "\u00c1");  // capital A, acute accent
        entMngr.defTextEntity("Acirc",  "\u00c2");   // capital A, circumflex accent
        entMngr.defTextEntity("Agrave", "\u00c0");  // capital A, grave accent
        entMngr.defTextEntity("Aring",  "\u00c5");   // capital A, ring
        entMngr.defTextEntity("Atilde", "\u00c3");  // capital A, tilde
        entMngr.defTextEntity("Auml",   "\u00c4");    // capital A, dieresis or umlaut mark
        entMngr.defTextEntity("Ccedil", "\u00c7");  // capital C, cedilla
        entMngr.defTextEntity("ETH",    "\u00d0");     // capital Eth, Icelandic
        entMngr.defTextEntity("Eacute", "\u00c9");  // capital E, acute accent
        entMngr.defTextEntity("Ecirc",  "\u00ca");   // capital E, circumflex accent
        entMngr.defTextEntity("Egrave", "\u00c8");  // capital E, grave accent
        entMngr.defTextEntity("Euml",   "\u00cb");    // capital E, dieresis or umlaut mark
        entMngr.defTextEntity("Iacute", "\u00cd");  // capital I, acute accent
        entMngr.defTextEntity("Icirc",  "\u00ce");   // capital I, circumflex accent
        entMngr.defTextEntity("Igrave", "\u00cc");  // capital I, grave accent
        entMngr.defTextEntity("Iuml",   "\u00cf");    // capital I, dieresis or umlaut mark
        entMngr.defTextEntity("Ntilde", "\u00d1");  // capital N, tilde
        entMngr.defTextEntity("Oacute", "\u00d3");  // capital O, acute accent
        entMngr.defTextEntity("Ocirc",  "\u00d4");   // capital O, circumflex accent
        entMngr.defTextEntity("Ograve", "\u00d2");  // capital O, grave accent
        entMngr.defTextEntity("Oslash", "\u00d8");  // capital O, slash
        entMngr.defTextEntity("Otilde", "\u00d5");  // capital O, tilde
        entMngr.defTextEntity("Ouml",   "\u00d6");    // capital O, dieresis or umlaut mark
        entMngr.defTextEntity("THORN",  "\u00de");   // capital THORN, Icelandic
        entMngr.defTextEntity("Uacute", "\u00da");  // capital U, acute accent
        entMngr.defTextEntity("Ucirc",  "\u00db");   // capital U, circumflex accent
        entMngr.defTextEntity("Ugrave", "\u00d9");  // capital U, grave accent
        entMngr.defTextEntity("Uuml",   "\u00dc");    // capital U, dieresis or umlaut mark
        entMngr.defTextEntity("Yacute", "\u00dd");  // capital Y, acute accent
        entMngr.defTextEntity("aacute", "\u00e1");  // small a, acute accent
        entMngr.defTextEntity("acirc",  "\u00e2");   // small a, circumflex accent
        entMngr.defTextEntity("aelig",  "\u00e6");   // small ae diphthong (ligature)
        entMngr.defTextEntity("agrave", "\u00e0");  // small a, grave accent
        entMngr.defTextEntity("aring",  "\u00e5");   // small a, ring
        entMngr.defTextEntity("atilde", "\u00e3");  // small a, tilde
        entMngr.defTextEntity("auml",   "\u00e4");    // small a, dieresis or umlaut mark
        entMngr.defTextEntity("ccedil", "\u00e7");  // small c, cedilla
        entMngr.defTextEntity("eacute", "\u00e9");  // small e, acute accent
        entMngr.defTextEntity("ecirc",  "\u00ea");   // small e, circumflex accent
        entMngr.defTextEntity("egrave", "\u00e8");  // small e, grave accent
        entMngr.defTextEntity("eth",    "\u00f0");     // small eth, Icelandic
        entMngr.defTextEntity("euml",   "\u00eb");    // small e, dieresis or umlaut mark
        entMngr.defTextEntity("iacute", "\u00ed");  // small i, acute accent
        entMngr.defTextEntity("icirc",  "\u00ee");   // small i, circumflex accent
        entMngr.defTextEntity("igrave", "\u00ec");  // small i, grave accent
        entMngr.defTextEntity("iuml",   "\u00ef");    // small i, dieresis or umlaut mark
        entMngr.defTextEntity("ntilde", "\u00f1");  // small n, tilde
        entMngr.defTextEntity("oacute", "\u00f3");  // small o, acute accent
        entMngr.defTextEntity("ocirc",  "\u00f4");   // small o, circumflex accent
        entMngr.defTextEntity("ograve", "\u00f2");  // small o, grave accent
        entMngr.defTextEntity("oslash", "\u00f8");  // small o, slash
        entMngr.defTextEntity("otilde", "\u00f5");  // small o, tilde
        entMngr.defTextEntity("ouml",   "\u00f6");    // small o, dieresis or umlaut mark
        entMngr.defTextEntity("szlig",  "\u00df");   // small sharp s, German (sz ligature)
        entMngr.defTextEntity("thorn",  "\u00fe");   // small thorn, Icelandic
        entMngr.defTextEntity("uacute", "\u00fa");  // small u, acute accent
        entMngr.defTextEntity("ucirc",  "\u00fb");   // small u, circumflex accent
        entMngr.defTextEntity("ugrave", "\u00f9");  // small u, grave accent
        entMngr.defTextEntity("uuml",   "\u00fc");    // small u, dieresis or umlaut mark
        entMngr.defTextEntity("yacute", "\u00fd");  // small y, acute accent
        entMngr.defTextEntity("yuml",   "\u00ff");    // small y, dieresis or umlaut mark

        // Some extra Latin 1 chars that are listed in the HTML3.2 draft (21-May-96)
        entMngr.defTextEntity("nbsp",   "\u00a0");  // non breaking space
        entMngr.defTextEntity("reg",    "\u00ae");   // registered sign
        entMngr.defTextEntity("copy",   "\u00a9");  // copyright sign

        // Additional ISO-8859/1     entities listed in rfc1866 (section 14)
        entMngr.defTextEntity("iexcl",  "\u00a1");
        entMngr.defTextEntity("cent",   "\u00a2");
        entMngr.defTextEntity("pound",  "\u00a3");
        entMngr.defTextEntity("curren", "\u00a4");
        entMngr.defTextEntity("yen",    "\u00a5");
        entMngr.defTextEntity("brvbar", "\u00a6");
        entMngr.defTextEntity("sect",   "\u00a7");
        entMngr.defTextEntity("uml",    "\u00a8");
        entMngr.defTextEntity("ordf",   "\u00aa");
        entMngr.defTextEntity("laquo",  "\u00ab");
        entMngr.defTextEntity("not",    "\u00ac");
        entMngr.defTextEntity("shy",    "\u00ad");  // soft hyphen
        entMngr.defTextEntity("macr",   "\u00af");
        entMngr.defTextEntity("deg",    "\u00b0");
        entMngr.defTextEntity("plusmn", "\u00b1");
        entMngr.defTextEntity("sup1",   "\u00b9");
        entMngr.defTextEntity("sup2",   "\u00b2");
        entMngr.defTextEntity("sup3",   "\u00b3");
        entMngr.defTextEntity("acute",  "\u00b4");
        entMngr.defTextEntity("micro",  "\u00b5");
        entMngr.defTextEntity("para",   "\u00b6");
        entMngr.defTextEntity("middot", "\u00b7");
        entMngr.defTextEntity("cedil",  "\u00b8");
        entMngr.defTextEntity("ordm",   "\u00ba");
        entMngr.defTextEntity("raquo",  "\u00bb");
        entMngr.defTextEntity("frac14", "\u00bc");
        entMngr.defTextEntity("frac12", "\u00bd");
        entMngr.defTextEntity("frac34", "\u00be");
        entMngr.defTextEntity("iquest", "\u00bf");
        entMngr.defTextEntity("times",  "\u00d7");
        entMngr.defTextEntity("divide", "\u00f7");
    }

    public static void applyHacks(Parser parser) {
        parser.addEmptyElms(emptyElms);
        parser.setElmTerminators(LI, li_terminators);
        parser.setElmTerminators(DT, dt_terminators);
        parser.setElmTerminators(DD, dd_terminators);
        parser.setElmTerminators(P, p_terminators);
        //parser.ignoreWS(ginore_ws);
        applyHacks(parser.getTokenizer());
    }
}
