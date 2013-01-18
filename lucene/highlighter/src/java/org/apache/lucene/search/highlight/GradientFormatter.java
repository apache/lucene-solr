package org.apache.lucene.search.highlight;
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

/**
 * Formats text with different color intensity depending on the score of the
 * term.
 *
 */
public class GradientFormatter implements Formatter
{
    private float maxScore;

    int fgRMin, fgGMin, fgBMin;

    int fgRMax, fgGMax, fgBMax;

    protected boolean highlightForeground;

    int bgRMin, bgGMin, bgBMin;

    int bgRMax, bgGMax, bgBMax;

    protected boolean highlightBackground;

    /**
     * Sets the color range for the IDF scores
     * 
     * @param maxScore
     *            The score (and above) displayed as maxColor (See QueryScorer.getMaxWeight 
     *         which can be used to calibrate scoring scale)
     * @param minForegroundColor
     *            The hex color used for representing IDF scores of zero eg
     *            #FFFFFF (white) or null if no foreground color required
     * @param maxForegroundColor
     *            The largest hex color used for representing IDF scores eg
     *            #000000 (black) or null if no foreground color required
     * @param minBackgroundColor
     *            The hex color used for representing IDF scores of zero eg
     *            #FFFFFF (white) or null if no background color required
     * @param maxBackgroundColor
     *            The largest hex color used for representing IDF scores eg
     *            #000000 (black) or null if no background color required
     */
    public GradientFormatter(float maxScore, String minForegroundColor,
            String maxForegroundColor, String minBackgroundColor,
            String maxBackgroundColor)
    {
        highlightForeground = (minForegroundColor != null)
                && (maxForegroundColor != null);
        if (highlightForeground)
        {
            if (minForegroundColor.length() != 7)
            {
                throw new IllegalArgumentException(
                        "minForegroundColor is not 7 bytes long eg a hex "
                                + "RGB value such as #FFFFFF");
            }
            if (maxForegroundColor.length() != 7)
            {
                throw new IllegalArgumentException(
                        "minForegroundColor is not 7 bytes long eg a hex "
                                + "RGB value such as #FFFFFF");
            }
            fgRMin = hexToInt(minForegroundColor.substring(1, 3));
            fgGMin = hexToInt(minForegroundColor.substring(3, 5));
            fgBMin = hexToInt(minForegroundColor.substring(5, 7));

            fgRMax = hexToInt(maxForegroundColor.substring(1, 3));
            fgGMax = hexToInt(maxForegroundColor.substring(3, 5));
            fgBMax = hexToInt(maxForegroundColor.substring(5, 7));
        }

        highlightBackground = (minBackgroundColor != null)
                && (maxBackgroundColor != null);
        if (highlightBackground)
        {
            if (minBackgroundColor.length() != 7)
            {
                throw new IllegalArgumentException(
                        "minBackgroundColor is not 7 bytes long eg a hex "
                                + "RGB value such as #FFFFFF");
            }
            if (maxBackgroundColor.length() != 7)
            {
                throw new IllegalArgumentException(
                        "minBackgroundColor is not 7 bytes long eg a hex "
                                + "RGB value such as #FFFFFF");
            }
            bgRMin = hexToInt(minBackgroundColor.substring(1, 3));
            bgGMin = hexToInt(minBackgroundColor.substring(3, 5));
            bgBMin = hexToInt(minBackgroundColor.substring(5, 7));

            bgRMax = hexToInt(maxBackgroundColor.substring(1, 3));
            bgGMax = hexToInt(maxBackgroundColor.substring(3, 5));
            bgBMax = hexToInt(maxBackgroundColor.substring(5, 7));
        }
        //        this.corpusReader = corpusReader;
        this.maxScore = maxScore;
        //        totalNumDocs = corpusReader.numDocs();
    }

    @Override
    public String highlightTerm(String originalText, TokenGroup tokenGroup)
    {
        if (tokenGroup.getTotalScore() == 0)
            return originalText;
        float score = tokenGroup.getTotalScore();
        if (score == 0)
        {
            return originalText;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("<font ");
        if (highlightForeground)
        {
            sb.append("color=\"");
            sb.append(getForegroundColorString(score));
            sb.append("\" ");
        }
        if (highlightBackground)
        {
            sb.append("bgcolor=\"");
            sb.append(getBackgroundColorString(score));
            sb.append("\" ");
        }
        sb.append(">");
        sb.append(originalText);
        sb.append("</font>");
        return sb.toString();
    }

    protected String getForegroundColorString(float score)
    {
        int rVal = getColorVal(fgRMin, fgRMax, score);
        int gVal = getColorVal(fgGMin, fgGMax, score);
        int bVal = getColorVal(fgBMin, fgBMax, score);
        StringBuilder sb = new StringBuilder();
        sb.append("#");
        sb.append(intToHex(rVal));
        sb.append(intToHex(gVal));
        sb.append(intToHex(bVal));
        return sb.toString();
    }

    protected String getBackgroundColorString(float score)
    {
        int rVal = getColorVal(bgRMin, bgRMax, score);
        int gVal = getColorVal(bgGMin, bgGMax, score);
        int bVal = getColorVal(bgBMin, bgBMax, score);
        StringBuilder sb = new StringBuilder();
        sb.append("#");
        sb.append(intToHex(rVal));
        sb.append(intToHex(gVal));
        sb.append(intToHex(bVal));
        return sb.toString();
    }

    private int getColorVal(int colorMin, int colorMax, float score)
    {
        if (colorMin == colorMax)
        {
            return colorMin;
        }
        float scale = Math.abs(colorMin - colorMax);
        float relScorePercent = Math.min(maxScore, score) / maxScore;
        float colScore = scale * relScorePercent;
        return Math.min(colorMin, colorMax) + (int) colScore;
    }

    private static char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    private static String intToHex(int i)
    {
        return "" + hexDigits[(i & 0xF0) >> 4] + hexDigits[i & 0x0F];
    }

    /**
     * Converts a hex string into an int. Integer.parseInt(hex, 16) assumes the
     * input is nonnegative unless there is a preceding minus sign. This method
     * reads the input as twos complement instead, so if the input is 8 bytes
     * long, it will correctly restore a negative int produced by
     * Integer.toHexString() but not necessarily one produced by
     * Integer.toString(x,16) since that method will produce a string like '-FF'
     * for negative integer values.
     * 
     * @param hex
     *            A string in capital or lower case hex, of no more then 16
     *            characters.
     * @throws NumberFormatException
     *             if the string is more than 16 characters long, or if any
     *             character is not in the set [0-9a-fA-f]
     */
    public static final int hexToInt(String hex)
    {
        int len = hex.length();
        if (len > 16)
            throw new NumberFormatException();

        int l = 0;
        for (int i = 0; i < len; i++)
        {
            l <<= 4;
            int c = Character.digit(hex.charAt(i), 16);
            if (c < 0)
                throw new NumberFormatException();
            l |= c;
        }
        return l;
    }

}

