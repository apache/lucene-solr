/*****************************************************************************
 * Copyright (C) The Apache Software Foundation. All rights reserved.        *
 * ------------------------------------------------------------------------- *
 * This software is published under the terms of the Apache Software License *
 * version 1.1, a copy of which has been included  with this distribution in *
 * the LICENSE file.                                                         *
 *****************************************************************************/
package org.krysalis.centipede.ant.task;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import org.apache.tools.ant.taskdefs.Property;

/**
 * Task to ask property values to the user. Uses current value as default.
 *
 * @author <a href="mailto:barozzi@nicolaken.com">Nicola Ken Barozzi</a>
 * @created 14 January 2002
 * @version CVS $Revision$ $Date$
 */
public class UserInputTask extends org.apache.tools.ant.Task {

  private String question;
  private String name;
  private String value;

  /**
   * Constructor.
   */
  public UserInputTask() {
    super();
  }

  /**
   * Initializes the task.
   */
  public void init() {
    super.init();
    question = "?";
  }

  /**
   * Run the task.
   * @exception org.apache.tools.ant.BuildException The exception raised during task execution.
   */
  public void execute() throws org.apache.tools.ant.BuildException {
    value = project.getProperty(name);
    String defaultvalue = value;

    //if the property exists
    if (value != null) {

      System.out.println("\n"+question + " ["+value + "] ");

      BufferedReader reader = new BufferedReader(new InputStreamReader (System.in));

      try
      {
        value = reader.readLine();
      }
      catch (IOException e)
      {
        value = defaultvalue;
      }

      if (!value.equals("")) {
        project.setProperty(name, value);
      } else {
        project.setProperty(name, defaultvalue);
      }
    }
  }

  /**
   * Sets the prompt text that will be presented to the user.
   * @param prompt String
   */
  public void addText(String question) {
    this.question=question;
  }

  public void setQuestion(String question) {
    this.question = question;
  }

  public void setName(String name) {
    this.name = name;
  }

}

