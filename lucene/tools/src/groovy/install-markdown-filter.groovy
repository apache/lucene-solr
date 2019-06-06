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

/** Task script that is called by Ant's common-build.xml file:
 * Installs markdown filter into Ant.
 */

import org.apache.tools.ant.AntTypeDefinition;
import org.apache.tools.ant.ComponentHelper;
import org.apache.tools.ant.filters.TokenFilter.ChainableReaderFilter;
import com.vladsch.flexmark.util.ast.Document;
import com.vladsch.flexmark.ast.Heading;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.parser.ParserEmulationProfile;
import com.vladsch.flexmark.util.html.Escaping;
import com.vladsch.flexmark.util.options.MutableDataSet;
import com.vladsch.flexmark.ext.abbreviation.AbbreviationExtension;
import com.vladsch.flexmark.ext.autolink.AutolinkExtension;

public final class MarkdownFilter extends ChainableReaderFilter {
  @Override
  public String filter(String markdownSource) {
    MutableDataSet options = new MutableDataSet();
    options.setFrom(ParserEmulationProfile.MARKDOWN);
    options.set(Parser.EXTENSIONS, [ AbbreviationExtension.create(), AutolinkExtension.create() ]);
    options.set(HtmlRenderer.RENDER_HEADER_ID, true);
    options.set(HtmlRenderer.MAX_TRAILING_BLANK_LINES, 0);
    Document parsed = Parser.builder(options).build().parse(markdownSource);

    StringBuilder html = new StringBuilder('<html>\n<head>\n');
    CharSequence title = parsed.getFirstChildAny(Heading.class)?.getText();          
    if (title != null) {
      html.append('<title>').append(Escaping.escapeHtml(title, false)).append('</title>\n');
    }
    html.append('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">\n')
      .append('</head>\n<body>\n');
    HtmlRenderer.builder(options).build().render(parsed, html);
    html.append('</body>\n</html>\n');
    return html;
  }
}

AntTypeDefinition t = new AntTypeDefinition();
t.setName('markdownfilter');
t.setClass(MarkdownFilter.class);
ComponentHelper.getComponentHelper(project).addDataTypeDefinition(t);
