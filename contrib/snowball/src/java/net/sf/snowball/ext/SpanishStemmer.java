// This file was generated automatically by the Snowball to Java compiler

package net.sf.snowball.ext;
import net.sf.snowball.SnowballProgram;
import net.sf.snowball.Among;

/**
 * Generated class implementing code defined by a snowball script.
 */
public class SpanishStemmer extends SnowballProgram {

        private Among a_0[] = {
            new Among ( "", -1, 6, "", this),
            new Among ( "\u00E1", 0, 1, "", this),
            new Among ( "\u00E9", 0, 2, "", this),
            new Among ( "\u00ED", 0, 3, "", this),
            new Among ( "\u00F3", 0, 4, "", this),
            new Among ( "\u00FA", 0, 5, "", this)
        };

        private Among a_1[] = {
            new Among ( "la", -1, -1, "", this),
            new Among ( "sela", 0, -1, "", this),
            new Among ( "le", -1, -1, "", this),
            new Among ( "me", -1, -1, "", this),
            new Among ( "se", -1, -1, "", this),
            new Among ( "lo", -1, -1, "", this),
            new Among ( "selo", 5, -1, "", this),
            new Among ( "las", -1, -1, "", this),
            new Among ( "selas", 7, -1, "", this),
            new Among ( "les", -1, -1, "", this),
            new Among ( "los", -1, -1, "", this),
            new Among ( "selos", 10, -1, "", this),
            new Among ( "nos", -1, -1, "", this)
        };

        private Among a_2[] = {
            new Among ( "ando", -1, 6, "", this),
            new Among ( "iendo", -1, 6, "", this),
            new Among ( "yendo", -1, 7, "", this),
            new Among ( "\u00E1ndo", -1, 2, "", this),
            new Among ( "i\u00E9ndo", -1, 1, "", this),
            new Among ( "ar", -1, 6, "", this),
            new Among ( "er", -1, 6, "", this),
            new Among ( "ir", -1, 6, "", this),
            new Among ( "\u00E1r", -1, 3, "", this),
            new Among ( "\u00E9r", -1, 4, "", this),
            new Among ( "\u00EDr", -1, 5, "", this)
        };

        private Among a_3[] = {
            new Among ( "ic", -1, -1, "", this),
            new Among ( "ad", -1, -1, "", this),
            new Among ( "os", -1, -1, "", this),
            new Among ( "iv", -1, 1, "", this)
        };

        private Among a_4[] = {
            new Among ( "able", -1, 1, "", this),
            new Among ( "ible", -1, 1, "", this)
        };

        private Among a_5[] = {
            new Among ( "ic", -1, 1, "", this),
            new Among ( "abil", -1, 1, "", this),
            new Among ( "iv", -1, 1, "", this)
        };

        private Among a_6[] = {
            new Among ( "ica", -1, 1, "", this),
            new Among ( "encia", -1, 5, "", this),
            new Among ( "adora", -1, 2, "", this),
            new Among ( "osa", -1, 1, "", this),
            new Among ( "ista", -1, 1, "", this),
            new Among ( "iva", -1, 9, "", this),
            new Among ( "anza", -1, 1, "", this),
            new Among ( "log\u00EDa", -1, 3, "", this),
            new Among ( "idad", -1, 8, "", this),
            new Among ( "able", -1, 1, "", this),
            new Among ( "ible", -1, 1, "", this),
            new Among ( "mente", -1, 7, "", this),
            new Among ( "amente", 11, 6, "", this),
            new Among ( "aci\u00F3n", -1, 2, "", this),
            new Among ( "uci\u00F3n", -1, 4, "", this),
            new Among ( "ico", -1, 1, "", this),
            new Among ( "ismo", -1, 1, "", this),
            new Among ( "oso", -1, 1, "", this),
            new Among ( "amiento", -1, 1, "", this),
            new Among ( "imiento", -1, 1, "", this),
            new Among ( "ivo", -1, 9, "", this),
            new Among ( "ador", -1, 2, "", this),
            new Among ( "icas", -1, 1, "", this),
            new Among ( "encias", -1, 5, "", this),
            new Among ( "adoras", -1, 2, "", this),
            new Among ( "osas", -1, 1, "", this),
            new Among ( "istas", -1, 1, "", this),
            new Among ( "ivas", -1, 9, "", this),
            new Among ( "anzas", -1, 1, "", this),
            new Among ( "log\u00EDas", -1, 3, "", this),
            new Among ( "idades", -1, 8, "", this),
            new Among ( "ables", -1, 1, "", this),
            new Among ( "ibles", -1, 1, "", this),
            new Among ( "aciones", -1, 2, "", this),
            new Among ( "uciones", -1, 4, "", this),
            new Among ( "adores", -1, 2, "", this),
            new Among ( "icos", -1, 1, "", this),
            new Among ( "ismos", -1, 1, "", this),
            new Among ( "osos", -1, 1, "", this),
            new Among ( "amientos", -1, 1, "", this),
            new Among ( "imientos", -1, 1, "", this),
            new Among ( "ivos", -1, 9, "", this)
        };

        private Among a_7[] = {
            new Among ( "ya", -1, 1, "", this),
            new Among ( "ye", -1, 1, "", this),
            new Among ( "yan", -1, 1, "", this),
            new Among ( "yen", -1, 1, "", this),
            new Among ( "yeron", -1, 1, "", this),
            new Among ( "yendo", -1, 1, "", this),
            new Among ( "yo", -1, 1, "", this),
            new Among ( "yas", -1, 1, "", this),
            new Among ( "yes", -1, 1, "", this),
            new Among ( "yais", -1, 1, "", this),
            new Among ( "yamos", -1, 1, "", this),
            new Among ( "y\u00F3", -1, 1, "", this)
        };

        private Among a_8[] = {
            new Among ( "aba", -1, 2, "", this),
            new Among ( "ada", -1, 2, "", this),
            new Among ( "ida", -1, 2, "", this),
            new Among ( "ara", -1, 2, "", this),
            new Among ( "iera", -1, 2, "", this),
            new Among ( "\u00EDa", -1, 2, "", this),
            new Among ( "ar\u00EDa", 5, 2, "", this),
            new Among ( "er\u00EDa", 5, 2, "", this),
            new Among ( "ir\u00EDa", 5, 2, "", this),
            new Among ( "ad", -1, 2, "", this),
            new Among ( "ed", -1, 2, "", this),
            new Among ( "id", -1, 2, "", this),
            new Among ( "ase", -1, 2, "", this),
            new Among ( "iese", -1, 2, "", this),
            new Among ( "aste", -1, 2, "", this),
            new Among ( "iste", -1, 2, "", this),
            new Among ( "an", -1, 2, "", this),
            new Among ( "aban", 16, 2, "", this),
            new Among ( "aran", 16, 2, "", this),
            new Among ( "ieran", 16, 2, "", this),
            new Among ( "\u00EDan", 16, 2, "", this),
            new Among ( "ar\u00EDan", 20, 2, "", this),
            new Among ( "er\u00EDan", 20, 2, "", this),
            new Among ( "ir\u00EDan", 20, 2, "", this),
            new Among ( "en", -1, 1, "", this),
            new Among ( "asen", 24, 2, "", this),
            new Among ( "iesen", 24, 2, "", this),
            new Among ( "aron", -1, 2, "", this),
            new Among ( "ieron", -1, 2, "", this),
            new Among ( "ar\u00E1n", -1, 2, "", this),
            new Among ( "er\u00E1n", -1, 2, "", this),
            new Among ( "ir\u00E1n", -1, 2, "", this),
            new Among ( "ado", -1, 2, "", this),
            new Among ( "ido", -1, 2, "", this),
            new Among ( "ando", -1, 2, "", this),
            new Among ( "iendo", -1, 2, "", this),
            new Among ( "ar", -1, 2, "", this),
            new Among ( "er", -1, 2, "", this),
            new Among ( "ir", -1, 2, "", this),
            new Among ( "as", -1, 2, "", this),
            new Among ( "abas", 39, 2, "", this),
            new Among ( "adas", 39, 2, "", this),
            new Among ( "idas", 39, 2, "", this),
            new Among ( "aras", 39, 2, "", this),
            new Among ( "ieras", 39, 2, "", this),
            new Among ( "\u00EDas", 39, 2, "", this),
            new Among ( "ar\u00EDas", 45, 2, "", this),
            new Among ( "er\u00EDas", 45, 2, "", this),
            new Among ( "ir\u00EDas", 45, 2, "", this),
            new Among ( "es", -1, 1, "", this),
            new Among ( "ases", 49, 2, "", this),
            new Among ( "ieses", 49, 2, "", this),
            new Among ( "abais", -1, 2, "", this),
            new Among ( "arais", -1, 2, "", this),
            new Among ( "ierais", -1, 2, "", this),
            new Among ( "\u00EDais", -1, 2, "", this),
            new Among ( "ar\u00EDais", 55, 2, "", this),
            new Among ( "er\u00EDais", 55, 2, "", this),
            new Among ( "ir\u00EDais", 55, 2, "", this),
            new Among ( "aseis", -1, 2, "", this),
            new Among ( "ieseis", -1, 2, "", this),
            new Among ( "asteis", -1, 2, "", this),
            new Among ( "isteis", -1, 2, "", this),
            new Among ( "\u00E1is", -1, 2, "", this),
            new Among ( "\u00E9is", -1, 1, "", this),
            new Among ( "ar\u00E9is", 64, 2, "", this),
            new Among ( "er\u00E9is", 64, 2, "", this),
            new Among ( "ir\u00E9is", 64, 2, "", this),
            new Among ( "ados", -1, 2, "", this),
            new Among ( "idos", -1, 2, "", this),
            new Among ( "amos", -1, 2, "", this),
            new Among ( "\u00E1bamos", 70, 2, "", this),
            new Among ( "\u00E1ramos", 70, 2, "", this),
            new Among ( "i\u00E9ramos", 70, 2, "", this),
            new Among ( "\u00EDamos", 70, 2, "", this),
            new Among ( "ar\u00EDamos", 74, 2, "", this),
            new Among ( "er\u00EDamos", 74, 2, "", this),
            new Among ( "ir\u00EDamos", 74, 2, "", this),
            new Among ( "emos", -1, 1, "", this),
            new Among ( "aremos", 78, 2, "", this),
            new Among ( "eremos", 78, 2, "", this),
            new Among ( "iremos", 78, 2, "", this),
            new Among ( "\u00E1semos", 78, 2, "", this),
            new Among ( "i\u00E9semos", 78, 2, "", this),
            new Among ( "imos", -1, 2, "", this),
            new Among ( "ar\u00E1s", -1, 2, "", this),
            new Among ( "er\u00E1s", -1, 2, "", this),
            new Among ( "ir\u00E1s", -1, 2, "", this),
            new Among ( "\u00EDs", -1, 2, "", this),
            new Among ( "ar\u00E1", -1, 2, "", this),
            new Among ( "er\u00E1", -1, 2, "", this),
            new Among ( "ir\u00E1", -1, 2, "", this),
            new Among ( "ar\u00E9", -1, 2, "", this),
            new Among ( "er\u00E9", -1, 2, "", this),
            new Among ( "ir\u00E9", -1, 2, "", this),
            new Among ( "i\u00F3", -1, 2, "", this)
        };

        private Among a_9[] = {
            new Among ( "a", -1, 1, "", this),
            new Among ( "e", -1, 2, "", this),
            new Among ( "o", -1, 1, "", this),
            new Among ( "os", -1, 1, "", this),
            new Among ( "\u00E1", -1, 1, "", this),
            new Among ( "\u00E9", -1, 2, "", this),
            new Among ( "\u00ED", -1, 1, "", this),
            new Among ( "\u00F3", -1, 1, "", this)
        };

        private static final char g_v[] = {17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 17, 4, 10 };

        private int I_p2;
        private int I_p1;
        private int I_pV;

        private void copy_from(SpanishStemmer other) {
            I_p2 = other.I_p2;
            I_p1 = other.I_p1;
            I_pV = other.I_pV;
            super.copy_from(other);
        }

        private boolean r_mark_regions() {
            int v_1;
            int v_2;
            int v_3;
            int v_6;
            int v_8;
            // (, line 31
            I_pV = limit;
            I_p1 = limit;
            I_p2 = limit;
            // do, line 37
            v_1 = cursor;
            lab0: do {
                // (, line 37
                // or, line 39
                lab1: do {
                    v_2 = cursor;
                    lab2: do {
                        // (, line 38
                        if (!(in_grouping(g_v, 97, 252)))
                        {
                            break lab2;
                        }
                        // or, line 38
                        lab3: do {
                            v_3 = cursor;
                            lab4: do {
                                // (, line 38
                                if (!(out_grouping(g_v, 97, 252)))
                                {
                                    break lab4;
                                }
                                // gopast, line 38
                                golab5: while(true)
                                {
                                    lab6: do {
                                        if (!(in_grouping(g_v, 97, 252)))
                                        {
                                            break lab6;
                                        }
                                        break golab5;
                                    } while (false);
                                    if (cursor >= limit)
                                    {
                                        break lab4;
                                    }
                                    cursor++;
                                }
                                break lab3;
                            } while (false);
                            cursor = v_3;
                            // (, line 38
                            if (!(in_grouping(g_v, 97, 252)))
                            {
                                break lab2;
                            }
                            // gopast, line 38
                            golab7: while(true)
                            {
                                lab8: do {
                                    if (!(out_grouping(g_v, 97, 252)))
                                    {
                                        break lab8;
                                    }
                                    break golab7;
                                } while (false);
                                if (cursor >= limit)
                                {
                                    break lab2;
                                }
                                cursor++;
                            }
                        } while (false);
                        break lab1;
                    } while (false);
                    cursor = v_2;
                    // (, line 40
                    if (!(out_grouping(g_v, 97, 252)))
                    {
                        break lab0;
                    }
                    // or, line 40
                    lab9: do {
                        v_6 = cursor;
                        lab10: do {
                            // (, line 40
                            if (!(out_grouping(g_v, 97, 252)))
                            {
                                break lab10;
                            }
                            // gopast, line 40
                            golab11: while(true)
                            {
                                lab12: do {
                                    if (!(in_grouping(g_v, 97, 252)))
                                    {
                                        break lab12;
                                    }
                                    break golab11;
                                } while (false);
                                if (cursor >= limit)
                                {
                                    break lab10;
                                }
                                cursor++;
                            }
                            break lab9;
                        } while (false);
                        cursor = v_6;
                        // (, line 40
                        if (!(in_grouping(g_v, 97, 252)))
                        {
                            break lab0;
                        }
                        // next, line 40
                        if (cursor >= limit)
                        {
                            break lab0;
                        }
                        cursor++;
                    } while (false);
                } while (false);
                // setmark pV, line 41
                I_pV = cursor;
            } while (false);
            cursor = v_1;
            // do, line 43
            v_8 = cursor;
            lab13: do {
                // (, line 43
                // gopast, line 44
                golab14: while(true)
                {
                    lab15: do {
                        if (!(in_grouping(g_v, 97, 252)))
                        {
                            break lab15;
                        }
                        break golab14;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab13;
                    }
                    cursor++;
                }
                // gopast, line 44
                golab16: while(true)
                {
                    lab17: do {
                        if (!(out_grouping(g_v, 97, 252)))
                        {
                            break lab17;
                        }
                        break golab16;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab13;
                    }
                    cursor++;
                }
                // setmark p1, line 44
                I_p1 = cursor;
                // gopast, line 45
                golab18: while(true)
                {
                    lab19: do {
                        if (!(in_grouping(g_v, 97, 252)))
                        {
                            break lab19;
                        }
                        break golab18;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab13;
                    }
                    cursor++;
                }
                // gopast, line 45
                golab20: while(true)
                {
                    lab21: do {
                        if (!(out_grouping(g_v, 97, 252)))
                        {
                            break lab21;
                        }
                        break golab20;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab13;
                    }
                    cursor++;
                }
                // setmark p2, line 45
                I_p2 = cursor;
            } while (false);
            cursor = v_8;
            return true;
        }

        private boolean r_postlude() {
            int among_var;
            int v_1;
            // repeat, line 49
            replab0: while(true)
            {
                v_1 = cursor;
                lab1: do {
                    // (, line 49
                    // [, line 50
                    bra = cursor;
                    // substring, line 50
                    among_var = find_among(a_0, 6);
                    if (among_var == 0)
                    {
                        break lab1;
                    }
                    // ], line 50
                    ket = cursor;
                    switch(among_var) {
                        case 0:
                            break lab1;
                        case 1:
                            // (, line 51
                            // <-, line 51
                            slice_from("a");
                            break;
                        case 2:
                            // (, line 52
                            // <-, line 52
                            slice_from("e");
                            break;
                        case 3:
                            // (, line 53
                            // <-, line 53
                            slice_from("i");
                            break;
                        case 4:
                            // (, line 54
                            // <-, line 54
                            slice_from("o");
                            break;
                        case 5:
                            // (, line 55
                            // <-, line 55
                            slice_from("u");
                            break;
                        case 6:
                            // (, line 57
                            // next, line 57
                            if (cursor >= limit)
                            {
                                break lab1;
                            }
                            cursor++;
                            break;
                    }
                    continue replab0;
                } while (false);
                cursor = v_1;
                break replab0;
            }
            return true;
        }

        private boolean r_RV() {
            if (!(I_pV <= cursor))
            {
                return false;
            }
            return true;
        }

        private boolean r_R1() {
            if (!(I_p1 <= cursor))
            {
                return false;
            }
            return true;
        }

        private boolean r_R2() {
            if (!(I_p2 <= cursor))
            {
                return false;
            }
            return true;
        }

        private boolean r_attached_pronoun() {
            int among_var;
            // (, line 67
            // [, line 68
            ket = cursor;
            // substring, line 68
            if (find_among_b(a_1, 13) == 0)
            {
                return false;
            }
            // ], line 68
            bra = cursor;
            // substring, line 72
            among_var = find_among_b(a_2, 11);
            if (among_var == 0)
            {
                return false;
            }
            // call RV, line 72
            if (!r_RV())
            {
                return false;
            }
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 73
                    // ], line 73
                    bra = cursor;
                    // <-, line 73
                    slice_from("iendo");
                    break;
                case 2:
                    // (, line 74
                    // ], line 74
                    bra = cursor;
                    // <-, line 74
                    slice_from("ando");
                    break;
                case 3:
                    // (, line 75
                    // ], line 75
                    bra = cursor;
                    // <-, line 75
                    slice_from("ar");
                    break;
                case 4:
                    // (, line 76
                    // ], line 76
                    bra = cursor;
                    // <-, line 76
                    slice_from("er");
                    break;
                case 5:
                    // (, line 77
                    // ], line 77
                    bra = cursor;
                    // <-, line 77
                    slice_from("ir");
                    break;
                case 6:
                    // (, line 81
                    // delete, line 81
                    slice_del();
                    break;
                case 7:
                    // (, line 82
                    // literal, line 82
                    if (!(eq_s_b(1, "u")))
                    {
                        return false;
                    }
                    // delete, line 82
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_standard_suffix() {
            int among_var;
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            // (, line 86
            // [, line 87
            ket = cursor;
            // substring, line 87
            among_var = find_among_b(a_6, 42);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 87
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 98
                    // call R2, line 99
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 99
                    slice_del();
                    break;
                case 2:
                    // (, line 103
                    // call R2, line 104
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 104
                    slice_del();
                    // try, line 105
                    v_1 = limit - cursor;
                    lab0: do {
                        // (, line 105
                        // [, line 105
                        ket = cursor;
                        // literal, line 105
                        if (!(eq_s_b(2, "ic")))
                        {
                            cursor = limit - v_1;
                            break lab0;
                        }
                        // ], line 105
                        bra = cursor;
                        // call R2, line 105
                        if (!r_R2())
                        {
                            cursor = limit - v_1;
                            break lab0;
                        }
                        // delete, line 105
                        slice_del();
                    } while (false);
                    break;
                case 3:
                    // (, line 109
                    // call R2, line 110
                    if (!r_R2())
                    {
                        return false;
                    }
                    // <-, line 110
                    slice_from("log");
                    break;
                case 4:
                    // (, line 113
                    // call R2, line 114
                    if (!r_R2())
                    {
                        return false;
                    }
                    // <-, line 114
                    slice_from("u");
                    break;
                case 5:
                    // (, line 117
                    // call R2, line 118
                    if (!r_R2())
                    {
                        return false;
                    }
                    // <-, line 118
                    slice_from("ente");
                    break;
                case 6:
                    // (, line 121
                    // call R1, line 122
                    if (!r_R1())
                    {
                        return false;
                    }
                    // delete, line 122
                    slice_del();
                    // try, line 123
                    v_2 = limit - cursor;
                    lab1: do {
                        // (, line 123
                        // [, line 124
                        ket = cursor;
                        // substring, line 124
                        among_var = find_among_b(a_3, 4);
                        if (among_var == 0)
                        {
                            cursor = limit - v_2;
                            break lab1;
                        }
                        // ], line 124
                        bra = cursor;
                        // call R2, line 124
                        if (!r_R2())
                        {
                            cursor = limit - v_2;
                            break lab1;
                        }
                        // delete, line 124
                        slice_del();
                        switch(among_var) {
                            case 0:
                                cursor = limit - v_2;
                                break lab1;
                            case 1:
                                // (, line 125
                                // [, line 125
                                ket = cursor;
                                // literal, line 125
                                if (!(eq_s_b(2, "at")))
                                {
                                    cursor = limit - v_2;
                                    break lab1;
                                }
                                // ], line 125
                                bra = cursor;
                                // call R2, line 125
                                if (!r_R2())
                                {
                                    cursor = limit - v_2;
                                    break lab1;
                                }
                                // delete, line 125
                                slice_del();
                                break;
                        }
                    } while (false);
                    break;
                case 7:
                    // (, line 133
                    // call R2, line 134
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 134
                    slice_del();
                    // try, line 135
                    v_3 = limit - cursor;
                    lab2: do {
                        // (, line 135
                        // [, line 136
                        ket = cursor;
                        // substring, line 136
                        among_var = find_among_b(a_4, 2);
                        if (among_var == 0)
                        {
                            cursor = limit - v_3;
                            break lab2;
                        }
                        // ], line 136
                        bra = cursor;
                        switch(among_var) {
                            case 0:
                                cursor = limit - v_3;
                                break lab2;
                            case 1:
                                // (, line 138
                                // call R2, line 138
                                if (!r_R2())
                                {
                                    cursor = limit - v_3;
                                    break lab2;
                                }
                                // delete, line 138
                                slice_del();
                                break;
                        }
                    } while (false);
                    break;
                case 8:
                    // (, line 144
                    // call R2, line 145
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 145
                    slice_del();
                    // try, line 146
                    v_4 = limit - cursor;
                    lab3: do {
                        // (, line 146
                        // [, line 147
                        ket = cursor;
                        // substring, line 147
                        among_var = find_among_b(a_5, 3);
                        if (among_var == 0)
                        {
                            cursor = limit - v_4;
                            break lab3;
                        }
                        // ], line 147
                        bra = cursor;
                        switch(among_var) {
                            case 0:
                                cursor = limit - v_4;
                                break lab3;
                            case 1:
                                // (, line 150
                                // call R2, line 150
                                if (!r_R2())
                                {
                                    cursor = limit - v_4;
                                    break lab3;
                                }
                                // delete, line 150
                                slice_del();
                                break;
                        }
                    } while (false);
                    break;
                case 9:
                    // (, line 156
                    // call R2, line 157
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 157
                    slice_del();
                    // try, line 158
                    v_5 = limit - cursor;
                    lab4: do {
                        // (, line 158
                        // [, line 159
                        ket = cursor;
                        // literal, line 159
                        if (!(eq_s_b(2, "at")))
                        {
                            cursor = limit - v_5;
                            break lab4;
                        }
                        // ], line 159
                        bra = cursor;
                        // call R2, line 159
                        if (!r_R2())
                        {
                            cursor = limit - v_5;
                            break lab4;
                        }
                        // delete, line 159
                        slice_del();
                    } while (false);
                    break;
            }
            return true;
        }

        private boolean r_y_verb_suffix() {
            int among_var;
            int v_1;
            int v_2;
            // (, line 165
            // setlimit, line 166
            v_1 = limit - cursor;
            // tomark, line 166
            if (cursor < I_pV)
            {
                return false;
            }
            cursor = I_pV;
            v_2 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_1;
            // (, line 166
            // [, line 166
            ket = cursor;
            // substring, line 166
            among_var = find_among_b(a_7, 12);
            if (among_var == 0)
            {
                limit_backward = v_2;
                return false;
            }
            // ], line 166
            bra = cursor;
            limit_backward = v_2;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 169
                    // literal, line 169
                    if (!(eq_s_b(1, "u")))
                    {
                        return false;
                    }
                    // delete, line 169
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_verb_suffix() {
            int among_var;
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            // (, line 173
            // setlimit, line 174
            v_1 = limit - cursor;
            // tomark, line 174
            if (cursor < I_pV)
            {
                return false;
            }
            cursor = I_pV;
            v_2 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_1;
            // (, line 174
            // [, line 174
            ket = cursor;
            // substring, line 174
            among_var = find_among_b(a_8, 96);
            if (among_var == 0)
            {
                limit_backward = v_2;
                return false;
            }
            // ], line 174
            bra = cursor;
            limit_backward = v_2;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 177
                    // try, line 177
                    v_3 = limit - cursor;
                    lab0: do {
                        // (, line 177
                        // literal, line 177
                        if (!(eq_s_b(1, "u")))
                        {
                            cursor = limit - v_3;
                            break lab0;
                        }
                        // test, line 177
                        v_4 = limit - cursor;
                        // literal, line 177
                        if (!(eq_s_b(1, "g")))
                        {
                            cursor = limit - v_3;
                            break lab0;
                        }
                        cursor = limit - v_4;
                    } while (false);
                    // ], line 177
                    bra = cursor;
                    // delete, line 177
                    slice_del();
                    break;
                case 2:
                    // (, line 198
                    // delete, line 198
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_residual_suffix() {
            int among_var;
            int v_1;
            int v_2;
            // (, line 202
            // [, line 203
            ket = cursor;
            // substring, line 203
            among_var = find_among_b(a_9, 8);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 203
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 206
                    // call RV, line 206
                    if (!r_RV())
                    {
                        return false;
                    }
                    // delete, line 206
                    slice_del();
                    break;
                case 2:
                    // (, line 208
                    // call RV, line 208
                    if (!r_RV())
                    {
                        return false;
                    }
                    // delete, line 208
                    slice_del();
                    // try, line 208
                    v_1 = limit - cursor;
                    lab0: do {
                        // (, line 208
                        // [, line 208
                        ket = cursor;
                        // literal, line 208
                        if (!(eq_s_b(1, "u")))
                        {
                            cursor = limit - v_1;
                            break lab0;
                        }
                        // ], line 208
                        bra = cursor;
                        // test, line 208
                        v_2 = limit - cursor;
                        // literal, line 208
                        if (!(eq_s_b(1, "g")))
                        {
                            cursor = limit - v_1;
                            break lab0;
                        }
                        cursor = limit - v_2;
                        // call RV, line 208
                        if (!r_RV())
                        {
                            cursor = limit - v_1;
                            break lab0;
                        }
                        // delete, line 208
                        slice_del();
                    } while (false);
                    break;
            }
            return true;
        }

        public boolean stem() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            // (, line 213
            // do, line 214
            v_1 = cursor;
            lab0: do {
                // call mark_regions, line 214
                if (!r_mark_regions())
                {
                    break lab0;
                }
            } while (false);
            cursor = v_1;
            // backwards, line 215
            limit_backward = cursor; cursor = limit;
            // (, line 215
            // do, line 216
            v_2 = limit - cursor;
            lab1: do {
                // call attached_pronoun, line 216
                if (!r_attached_pronoun())
                {
                    break lab1;
                }
            } while (false);
            cursor = limit - v_2;
            // do, line 217
            v_3 = limit - cursor;
            lab2: do {
                // (, line 217
                // or, line 217
                lab3: do {
                    v_4 = limit - cursor;
                    lab4: do {
                        // call standard_suffix, line 217
                        if (!r_standard_suffix())
                        {
                            break lab4;
                        }
                        break lab3;
                    } while (false);
                    cursor = limit - v_4;
                    lab5: do {
                        // call y_verb_suffix, line 218
                        if (!r_y_verb_suffix())
                        {
                            break lab5;
                        }
                        break lab3;
                    } while (false);
                    cursor = limit - v_4;
                    // call verb_suffix, line 219
                    if (!r_verb_suffix())
                    {
                        break lab2;
                    }
                } while (false);
            } while (false);
            cursor = limit - v_3;
            // do, line 221
            v_5 = limit - cursor;
            lab6: do {
                // call residual_suffix, line 221
                if (!r_residual_suffix())
                {
                    break lab6;
                }
            } while (false);
            cursor = limit - v_5;
            cursor = limit_backward;            // do, line 223
            v_6 = cursor;
            lab7: do {
                // call postlude, line 223
                if (!r_postlude())
                {
                    break lab7;
                }
            } while (false);
            cursor = v_6;
            return true;
        }

}

