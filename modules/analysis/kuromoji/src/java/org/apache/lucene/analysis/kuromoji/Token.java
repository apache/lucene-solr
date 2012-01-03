package org.apache.lucene.analysis.kuromoji;

/**
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

import org.apache.lucene.analysis.kuromoji.dict.Dictionary;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode.Type;

public class Token {
	private final Dictionary dictionary;

	private final int wordId;
	
	private final String surfaceForm;
	
	private final int position;
	
	private final Type type;
	
	public Token(int wordId, String surfaceForm, Type type, int position, Dictionary dictionary) {
		this.wordId = wordId;
		this.surfaceForm = surfaceForm;
		this.type = type;
		this.position = position;
		this.dictionary = dictionary;
	}

	/**
	 * @return surfaceForm
	 */
	public String getSurfaceForm() {
		return surfaceForm;
	}

	/**
	 * @return all features
	 */
	public String getAllFeatures() {
		return dictionary.getAllFeatures(wordId);
	}

	/**
	 * @return all features as array
	 */
	public String[] getAllFeaturesArray() {
		return dictionary.getAllFeaturesArray(wordId);
	}


	/**
	 * @return reading. null if token doesn't have reading.
	 */
	public String getReading() {
		return dictionary.getReading(wordId);
	}

	/**
	 * @return part of speech.
	 */
	public String getPartOfSpeech() {
		return dictionary.getPartOfSpeech(wordId);
	}

	/**
	 * Returns true if this token is known word
	 * @return true if this token is in standard dictionary. false if not.
	 */
	public boolean isKnown() {
		return type == Type.KNOWN;
	}

	/**
	 * Returns true if this token is unknown word
	 * @return true if this token is unknown word. false if not.
	 */
	public boolean isUnknown() {
		return type == Type.UNKNOWN;
	}
	
	/**
	 * Returns true if this token is defined in user dictionary
	 * @return true if this token is in user dictionary. false if not.
	 */
	public boolean isUser() {
		return type == Type.USER;
	}
	
	/**
	 * Get index of this token in input text
	 * @return position of token
	 */
	public int getPosition() {
		return position;
	}

}