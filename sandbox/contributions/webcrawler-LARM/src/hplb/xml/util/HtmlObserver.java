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

import java.net.URL;

/**
 * A callback interface used in conjunction with UrlScanner. Allows actions
 * to be taken whenever the scanner finds a URL in an HTML document. The
 * scanner knows about most HTML 4.0 elements which can contain URLs.
 * Can be used, for example, to implement robot code which crawls a hypertext
 * graph. This interface is similar to Jeff Poskanzer's Acme.HtmlObserver.
 * 
 * @see     HtmlScanner
 * @author  Anders Kristensen
 */
public interface HtmlObserver {
    /** Invoked when the scanner finds an &lt;a href=""&gt; URL. */
    public void gotAHref(String urlStr, URL contextUrl, Object data);

    /** Invoked when the scanner finds an &lt;img src=""&gt; URL. */
    public void gotImgSrc(String urlStr, URL contextUrl, Object data);

    /** Invoked when the scanner finds a &lt;base href=""&gt; URL. */
    public void gotBaseHref(String urlStr, URL contextUrl, Object data );

    /** Invoked when the scanner finds a &lt;area href=""&gt; URL. */
    public void gotAreaHref(String urlStr, URL contextUrl, Object data );

    /** Invoked when the scanner finds an &lt;frame src=""&gt; URL. */
    public void gotFrameSrc(String urlStr, URL contextUrl, Object data );
}
