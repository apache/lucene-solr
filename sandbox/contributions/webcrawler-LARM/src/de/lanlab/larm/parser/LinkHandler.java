
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c)<p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.parser;

public interface LinkHandler
{
    public void handleLink(String value, boolean isFrame);
    public void handleBase(String value);
    public void handleTitle(String value);
}