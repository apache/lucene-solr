package org.apache.regexp;

/**
 * This class exists as a gateway to access useful Jakarta Regexp package protected data.
 */
public class RegexpTunnel {
  public static char[] getPrefix(RE regexp) {
    REProgram program = regexp.getProgram();
    return program.prefix;
  }
}
