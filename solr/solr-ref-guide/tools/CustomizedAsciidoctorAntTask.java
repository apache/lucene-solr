/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.asciidoctor.ant.AsciidoctorAntTask;

/**
 * Customized version of the default AsciidoctorAntTask
 * To deal with the fact that we want sourceDocumentName="" treated the same as unspecified (ie: null)
 * in order to be able to wrap in a macro with defaults
 */
public class CustomizedAsciidoctorAntTask extends AsciidoctorAntTask {
  public void setSourceDocumentName(String sourceDocumentName) {
    if ("".equals(sourceDocumentName)) {
      sourceDocumentName = null;
    }
    super.setSourceDocumentName(sourceDocumentName);
  }
}

 
