package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Automaton-based tokenizer for testing. Optionally lowercases.
 */
public class MockTokenizer extends Tokenizer {
  /** Acts Similar to WhitespaceTokenizer */
  public static final CharacterRunAutomaton WHITESPACE = 
    new CharacterRunAutomaton(new RegExp("[^ \t\r\n]+").toAutomaton());
  /** Acts Similar to KeywordTokenizer.
   * TODO: Keyword returns an "empty" token for an empty reader... 
   */
  public static final CharacterRunAutomaton KEYWORD =
    new CharacterRunAutomaton(new RegExp(".*").toAutomaton());
  /** Acts like LetterTokenizer. */
  // the ugly regex below is Unicode 5.2 [:Letter:]
  public static final CharacterRunAutomaton SIMPLE =
    new CharacterRunAutomaton(new RegExp("[A-Za-zªµºÀ-ÖØ-öø-ˁˆ-ˑˠ-ˤˬˮͰ-ʹͶͷͺ-ͽΆΈ-ΊΌΎ-ΡΣ-ϵϷ-ҁҊ-ԥԱ-Ֆՙա-ևא-תװ-ײء-يٮٯٱ-ۓەۥۦۮۯۺ-ۼۿܐܒ-ܯݍ-ޥޱߊ-ߪߴߵߺࠀ-ࠕࠚࠤࠨऄ-हऽॐक़-ॡॱॲॹ-ॿঅ-ঌএঐও-নপ-রলশ-হঽৎড়ঢ়য়-ৡৰৱਅ-ਊਏਐਓ-ਨਪ-ਰਲਲ਼ਵਸ਼ਸਹਖ਼-ੜਫ਼ੲ-ੴઅ-ઍએ-ઑઓ-નપ-રલળવ-હઽૐૠૡଅ-ଌଏଐଓ-ନପ-ରଲଳଵ-ହଽଡ଼ଢ଼ୟ-ୡୱஃஅ-ஊஎ-ஐஒ-கஙசஜஞடணதந-பம-ஹௐఅ-ఌఎ-ఐఒ-నప-ళవ-హఽౘౙౠౡಅ-ಌಎ-ಐಒ-ನಪ-ಳವ-ಹಽೞೠೡഅ-ഌഎ-ഐഒ-നപ-ഹഽൠൡൺ-ൿඅ-ඖක-නඳ-රලව-ෆก-ะาำเ-ๆກຂຄງຈຊຍດ-ທນ-ຟມ-ຣລວສຫອ-ະາຳຽເ-ໄໆໜໝༀཀ-ཇཉ-ཬྈ-ྋက-ဪဿၐ-ၕၚ-ၝၡၥၦၮ-ၰၵ-ႁႎႠ-Ⴥა-ჺჼᄀ-ቈቊ-ቍቐ-ቖቘቚ-ቝበ-ኈኊ-ኍነ-ኰኲ-ኵኸ-ኾዀዂ-ዅወ-ዖዘ-ጐጒ-ጕጘ-ፚᎀ-ᎏᎠ-Ᏼᐁ-ᙬᙯ-ᙿᚁ-ᚚᚠ-ᛪᜀ-ᜌᜎ-ᜑᜠ-ᜱᝀ-ᝑᝠ-ᝬᝮ-ᝰក-ឳៗៜᠠ-ᡷᢀ-ᢨᢪᢰ-ᣵᤀ-ᤜᥐ-ᥭᥰ-ᥴᦀ-ᦫᧁ-ᧇᨀ-ᨖᨠ-ᩔᪧᬅ-ᬳᭅ-ᭋᮃ-ᮠᮮᮯᰀ-ᰣᱍ-ᱏᱚ-ᱽᳩ-ᳬᳮ-ᳱᴀ-ᶿḀ-ἕἘ-Ἕἠ-ὅὈ-Ὅὐ-ὗὙὛὝὟ-ώᾀ-ᾴᾶ-ᾼιῂ-ῄῆ-ῌῐ-ΐῖ-Ίῠ-Ῥῲ-ῴῶ-ῼⁱⁿₐ-ₔℂℇℊ-ℓℕℙ-ℝℤΩℨK-ℭℯ-ℹℼ-ℿⅅ-ⅉⅎↃↄⰀ-Ⱞⰰ-ⱞⱠ-ⳤⳫ-ⳮⴀ-ⴥⴰ-ⵥⵯⶀ-ⶖⶠ-ⶦⶨ-ⶮⶰ-ⶶⶸ-ⶾⷀ-ⷆⷈ-ⷎⷐ-ⷖⷘ-ⷞⸯ々〆〱-〵〻〼ぁ-ゖゝ-ゟァ-ヺー-ヿㄅ-ㄭㄱ-ㆎㆠ-ㆷㇰ-ㇿ㐀-䶵一-鿋ꀀ-ꒌꓐ-ꓽꔀ-ꘌꘐ-ꘟꘪꘫꙀ-ꙟꙢ-ꙮꙿ-ꚗꚠ-ꛥꜗ-ꜟꜢ-ꞈꞋꞌꟻ-ꠁꠃ-ꠅꠇ-ꠊꠌ-ꠢꡀ-ꡳꢂ-ꢳꣲ-ꣷꣻꤊ-ꤥꤰ-ꥆꥠ-ꥼꦄ-ꦲꧏꨀ-ꨨꩀ-ꩂꩄ-ꩋꩠ-ꩶꩺꪀ-ꪯꪱꪵꪶꪹ-ꪽꫀꫂꫛ-ꫝꯀ-ꯢ가-힣ힰ-ퟆퟋ-ퟻ豈-鶴侮-舘並-龎ﬀ-ﬆﬓ-ﬗיִײַ-ﬨשׁ-זּטּ-לּמּנּסּףּפּצּ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻﹰ-ﹴﹶ-ﻼＡ-Ｚａ-ｚｦ-ﾾￂ-ￇￊ-ￏￒ-ￗￚ-ￜ𐀀-𐀋𐀍-𐀦𐀨-𐀺𐀼𐀽𐀿-𐁍𐁐-𐁝𐂀-𐃺𐊀-𐊜𐊠-𐋐𐌀-𐌞𐌰-𐍀𐍂-𐍉𐎀-𐎝𐎠-𐏃𐏈-𐏏𐐀-𐒝𐠀-𐠅𐠈𐠊-𐠵𐠷𐠸𐠼𐠿-𐡕𐤀-𐤕𐤠-𐤹𐨀𐨐-𐨓𐨕-𐨗𐨙-𐨳𐩠-𐩼𐬀-𐬵𐭀-𐭕𐭠-𐭲𐰀-𐱈𑂃-𑂯𒀀-𒍮𓀀-𓐮𝐀-𝑔𝑖-𝒜𝒞𝒟𝒢𝒥𝒦𝒩-𝒬𝒮-𝒹𝒻𝒽-𝓃𝓅-𝔅𝔇-𝔊𝔍-𝔔𝔖-𝔜𝔞-𝔹𝔻-𝔾𝕀-𝕄𝕆𝕊-𝕐𝕒-𝚥𝚨-𝛀𝛂-𝛚𝛜-𝛺𝛼-𝜔𝜖-𝜴𝜶-𝝎𝝐-𝝮𝝰-𝞈𝞊-𝞨𝞪-𝟂𝟄-𝟋𠀀-𪛖𪜀-𫜴丽-𪘀]+").toAutomaton());

  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private int state;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  int off = 0;

  // TODO: "register" with LuceneTestCase to ensure all streams are closed() ?
  // currently, we can only check that the lifecycle is correct if someone is reusing,
  // but not for "one-offs".
  private static enum State { 
    SETREADER,       // consumer set a reader input either via ctor or via reset(Reader)
    RESET,           // consumer has called reset()
    INCREMENT,       // consumer is consuming, has called incrementToken() == true
    INCREMENT_FALSE, // consumer has called incrementToken() which returned false
    END,             // consumer has called end() to perform end of stream operations
    CLOSE            // consumer has called close() to release any resources
  };
  
  private State streamState = State.CLOSE;
  private boolean enableChecks = true;
  
  public MockTokenizer(AttributeFactory factory, Reader input, CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    super(factory, input);
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.state = runAutomaton.getInitialState();
    this.streamState = State.SETREADER;
  }

  public MockTokenizer(Reader input, CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    super(input);
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.state = runAutomaton.getInitialState();
    this.streamState = State.SETREADER;
  }
  
  @Override
  public final boolean incrementToken() throws IOException {
    assert !enableChecks || (streamState == State.RESET || streamState == State.INCREMENT) 
                            : "incrementToken() called while in wrong state: " + streamState;
    clearAttributes();
    for (;;) {
      int startOffset = off;
      int cp = readCodePoint();
      if (cp < 0) {
        break;
      } else if (isTokenChar(cp)) {
        int endOffset;
        do {
          char chars[] = Character.toChars(normalize(cp));
          for (int i = 0; i < chars.length; i++)
            termAtt.append(chars[i]);
          endOffset = off;
          cp = readCodePoint();
        } while (cp >= 0 && isTokenChar(cp));
        offsetAtt.setOffset(correctOffset(startOffset), correctOffset(endOffset));
        streamState = State.INCREMENT;
        return true;
      }
    }
    streamState = State.INCREMENT_FALSE;
    return false;
  }

  protected int readCodePoint() throws IOException {
    int ch = input.read();
    if (ch < 0) {
      return ch;
    } else {
      assert !Character.isLowSurrogate((char) ch);
      off++;
      if (Character.isHighSurrogate((char) ch)) {
        int ch2 = input.read();
        if (ch2 >= 0) {
          off++;
          assert Character.isLowSurrogate((char) ch2);
          return Character.toCodePoint((char) ch, (char) ch2);
        }
      }
      return ch;
    }
  }

  protected boolean isTokenChar(int c) {
    state = runAutomaton.step(state, c);
    if (state < 0) {
      state = runAutomaton.getInitialState();
      return false;
    } else {
      return true;
    }
  }
  
  protected int normalize(int c) {
    return lowerCase ? Character.toLowerCase(c) : c;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    state = runAutomaton.getInitialState();
    off = 0;
    assert !enableChecks || streamState != State.RESET : "double reset()";
    streamState = State.RESET;
  }
  
  @Override
  public void close() throws IOException {
    super.close();
    // in some exceptional cases (e.g. TestIndexWriterExceptions) a test can prematurely close()
    // these tests should disable this check, by default we check the normal workflow.
    // TODO: investigate the CachingTokenFilter "double-close"... for now we ignore this
    assert !enableChecks || streamState == State.END || streamState == State.CLOSE : "close() called in wrong state: " + streamState;
    streamState = State.CLOSE;
  }

  @Override
  public void reset(Reader input) throws IOException {
    super.reset(input);
    assert !enableChecks || streamState == State.CLOSE : "setReader() called in wrong state: " + streamState;
    streamState = State.SETREADER;
  }

  @Override
  public void end() throws IOException {
    int finalOffset = correctOffset(off);
    offsetAtt.setOffset(finalOffset, finalOffset);
    // some tokenizers, such as limiting tokenizers, call end() before incrementToken() returns false.
    // these tests should disable this check (in general you should consume the entire stream)
    assert !enableChecks || streamState == State.INCREMENT_FALSE : "end() called before incrementToken() returned false!";
    streamState = State.END;
  }

  /** 
   * Toggle consumer workflow checking: if your test consumes tokenstreams normally you
   * should leave this enabled.
   */
  public void setEnableChecks(boolean enableChecks) {
    this.enableChecks = enableChecks;
  }
}
