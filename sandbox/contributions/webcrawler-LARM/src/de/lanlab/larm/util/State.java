package de.lanlab.larm.util;

import java.io.Serializable;
/**
 * Title:        LARM Lanlab Retrieval Machine
 * Description:
 * Copyright:    Copyright (c)
 * Company:
 * @author
 * @version 1.0
 */

/**
 * thread safe state information.
 * The get methods are not synchronized. Clone the state object before using them
 * If you use a state object in a class, always return a clone
 * <pre>public class MyClass {
 *     State state = new State("Running");
 *     public State getState() { return state.cloneState() }</pre>
 *
 * note on serialization: if you deserialize a state, the state string will be newly created.
 * that means you then have to compare the states via equal() and not ==
 */
public class State implements Cloneable, Serializable
{

    private String state;
    private long stateSince;
    private Object info;

    public State(String state)
    {
        setState(state);
    }


    private State(String state, long stateSince)
    {
        init(state, stateSince, null);
    }

    private State(String state, long stateSince, Object info)
    {
        init(state, stateSince, info);
    }

    private void init(String state, long stateSince, Object info)
    {
        this.state = state;
        this.stateSince = stateSince;
        this.info = info;
    }

    public void setState(String state)
    {
        setState(state, null);
    }

    public synchronized void setState(String state, Object info)
    {
        this.state = state;
        this.stateSince = System.currentTimeMillis();
        this.info = info;
    }

    public String getState()
    {
        return state;
    }

    public long getStateSince()
    {
        return stateSince;
    }

    public Object getInfo()
    {
        return info;
    }

    public synchronized Object clone()
    {
        return new State(state, stateSince, info);
    }

    public State cloneState()
    {
        return (State)clone();
    }

}