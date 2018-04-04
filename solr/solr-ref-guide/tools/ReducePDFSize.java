/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.common.PDStream;

/**
 * A simple command line utility for reducing the size of the ref-guide PDF.
 * <p>
 * Currently this script focuses on using {@link COSName#FLATE_DECODE} to compress the (decoded) Objects 
 * in the source PDF, but other improvements may be possible in the future.
 * </p>
 * <p>
 * This code is originally based on the <code>WriteDecodedDoc</code> example provided with <a href="https://pdfbox.apache.org/">Apache PDFBox</a>.
 * </p>
 * <p>
 * <b>NOTE:</b> This class should <em>NOT</em> be considered a general purpose tool for reducing the size of 
 * <em>any</em> PDF.  
 * Decisions made in this code can and will be focused explicitly on serving the purpose of reducing the size of the 
 * Solr Reference Guide PDF, as originally produced by asciidoctor, and may not be generally useful for all PDFs 
 * "in the wild".
 * </p>
 */
public class ReducePDFSize {

  public static void main(String[] args) throws IOException {
    if (2 != args.length) {
      throw new RuntimeException("arg0 must be input file, org1 must be output file");
    }
    String in = args[0];
    String out = args[1];
    PDDocument doc = null;
    
    try {
      doc = PDDocument.load(new File(in));
      doc.setAllSecurityToBeRemoved(true);
      for (COSObject cosObject : doc.getDocument().getObjects()) {
        COSBase base = cosObject.getObject();
        // if it's a stream: decode it, then re-write it using FLATE_DECODE
        if (base instanceof COSStream) {
          COSStream stream = (COSStream) base;
          byte[] bytes;
          try {
            bytes = new PDStream(stream).toByteArray();
          } catch (IOException ex) {
            // NOTE: original example code from PDFBox just logged & "continue;"d here, 'skipping' this stream.
            // If this type of failure ever happens, we can (perhaps) consider (re)ignoring this type of failure?
            //
            // IIUC then that will leave the original (non-decoded / non-flated) stream in place?
            throw new RuntimeException("can't serialize byte[] from: " +
                                       cosObject.getObjectNumber() + " " + 
                                       cosObject.getGenerationNumber() + " obj: " + 
                                       ex.getMessage(), ex);
          }
          stream.removeItem(COSName.FILTER);
          OutputStream streamOut = stream.createOutputStream(COSName.FLATE_DECODE);
          streamOut.write(bytes);
          streamOut.close();
        }
      }
      doc.getDocumentCatalog();
      doc.save( out );
    } finally {
      if ( doc != null ) {
        doc.close();
      }
    }
  }
}
