
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c)<p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.fetcher;


/**
 * base class of all filter classes
 */
public abstract class Filter
{
	/**
	 * number of items filtered. augmented directly by
	 * the inheriting classes
	 */
    protected int filtered = 0;


    public int getFiltered()
    {
        return filtered;
    }
}