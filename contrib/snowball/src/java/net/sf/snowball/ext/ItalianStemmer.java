// This file was generated automatically by the Snowball to Java compiler

package net.sf.snowball.ext;
import net.sf.snowball.SnowballProgram;
import net.sf.snowball.Among;

/**
 * Generated class implementing code defined by a snowball script.
 */
public class ItalianStemmer extends SnowballProgram {

        private Among a_0[] = {
            new Among ( "", -1, 7, "", this),
            new Among ( "qu", 0, 6, "", this),
            new Among ( "\u00E1", 0, 1, "", this),
            new Among ( "\u00E9", 0, 2, "", this),
            new Among ( "\u00ED", 0, 3, "", this),
            new Among ( "\u00F3", 0, 4, "", this),
            new Among ( "\u00FA", 0, 5, "", this)
        };

        private Among a_1[] = {
            new Among ( "", -1, 3, "", this),
            new Among ( "I", 0, 1, "", this),
            new Among ( "U", 0, 2, "", this)
        };

        private Among a_2[] = {
            new Among ( "la", -1, -1, "", this),
            new Among ( "cela", 0, -1, "", this),
            new Among ( "gliela", 0, -1, "", this),
            new Among ( "mela", 0, -1, "", this),
            new Among ( "tela", 0, -1, "", this),
            new Among ( "vela", 0, -1, "", this),
            new Among ( "le", -1, -1, "", this),
            new Among ( "cele", 6, -1, "", this),
            new Among ( "gliele", 6, -1, "", this),
            new Among ( "mele", 6, -1, "", this),
            new Among ( "tele", 6, -1, "", this),
            new Among ( "vele", 6, -1, "", this),
            new Among ( "ne", -1, -1, "", this),
            new Among ( "cene", 12, -1, "", this),
            new Among ( "gliene", 12, -1, "", this),
            new Among ( "mene", 12, -1, "", this),
            new Among ( "sene", 12, -1, "", this),
            new Among ( "tene", 12, -1, "", this),
            new Among ( "vene", 12, -1, "", this),
            new Among ( "ci", -1, -1, "", this),
            new Among ( "li", -1, -1, "", this),
            new Among ( "celi", 20, -1, "", this),
            new Among ( "glieli", 20, -1, "", this),
            new Among ( "meli", 20, -1, "", this),
            new Among ( "teli", 20, -1, "", this),
            new Among ( "veli", 20, -1, "", this),
            new Among ( "gli", 20, -1, "", this),
            new Among ( "mi", -1, -1, "", this),
            new Among ( "si", -1, -1, "", this),
            new Among ( "ti", -1, -1, "", this),
            new Among ( "vi", -1, -1, "", this),
            new Among ( "lo", -1, -1, "", this),
            new Among ( "celo", 31, -1, "", this),
            new Among ( "glielo", 31, -1, "", this),
            new Among ( "melo", 31, -1, "", this),
            new Among ( "telo", 31, -1, "", this),
            new Among ( "velo", 31, -1, "", this)
        };

        private Among a_3[] = {
            new Among ( "ando", -1, 1, "", this),
            new Among ( "endo", -1, 1, "", this),
            new Among ( "ar", -1, 2, "", this),
            new Among ( "er", -1, 2, "", this),
            new Among ( "ir", -1, 2, "", this)
        };

        private Among a_4[] = {
            new Among ( "ic", -1, -1, "", this),
            new Among ( "abil", -1, -1, "", this),
            new Among ( "os", -1, -1, "", this),
            new Among ( "iv", -1, 1, "", this)
        };

        private Among a_5[] = {
            new Among ( "ic", -1, 1, "", this),
            new Among ( "abil", -1, 1, "", this),
            new Among ( "iv", -1, 1, "", this)
        };

        private Among a_6[] = {
            new Among ( "ica", -1, 1, "", this),
            new Among ( "logia", -1, 3, "", this),
            new Among ( "osa", -1, 1, "", this),
            new Among ( "ista", -1, 1, "", this),
            new Among ( "iva", -1, 9, "", this),
            new Among ( "anza", -1, 1, "", this),
            new Among ( "enza", -1, 5, "", this),
            new Among ( "ice", -1, 1, "", this),
            new Among ( "atrice", 7, 1, "", this),
            new Among ( "iche", -1, 1, "", this),
            new Among ( "logie", -1, 3, "", this),
            new Among ( "abile", -1, 1, "", this),
            new Among ( "ibile", -1, 1, "", this),
            new Among ( "usione", -1, 4, "", this),
            new Among ( "azione", -1, 2, "", this),
            new Among ( "uzione", -1, 4, "", this),
            new Among ( "atore", -1, 2, "", this),
            new Among ( "ose", -1, 1, "", this),
            new Among ( "mente", -1, 1, "", this),
            new Among ( "amente", 18, 7, "", this),
            new Among ( "iste", -1, 1, "", this),
            new Among ( "ive", -1, 9, "", this),
            new Among ( "anze", -1, 1, "", this),
            new Among ( "enze", -1, 5, "", this),
            new Among ( "ici", -1, 1, "", this),
            new Among ( "atrici", 24, 1, "", this),
            new Among ( "ichi", -1, 1, "", this),
            new Among ( "abili", -1, 1, "", this),
            new Among ( "ibili", -1, 1, "", this),
            new Among ( "ismi", -1, 1, "", this),
            new Among ( "usioni", -1, 4, "", this),
            new Among ( "azioni", -1, 2, "", this),
            new Among ( "uzioni", -1, 4, "", this),
            new Among ( "atori", -1, 2, "", this),
            new Among ( "osi", -1, 1, "", this),
            new Among ( "amenti", -1, 6, "", this),
            new Among ( "imenti", -1, 6, "", this),
            new Among ( "isti", -1, 1, "", this),
            new Among ( "ivi", -1, 9, "", this),
            new Among ( "ico", -1, 1, "", this),
            new Among ( "ismo", -1, 1, "", this),
            new Among ( "oso", -1, 1, "", this),
            new Among ( "amento", -1, 6, "", this),
            new Among ( "imento", -1, 6, "", this),
            new Among ( "ivo", -1, 9, "", this),
            new Among ( "it\u00E0", -1, 8, "", this),
            new Among ( "ist\u00E0", -1, 1, "", this),
            new Among ( "ist\u00E8", -1, 1, "", this),
            new Among ( "ist\u00EC", -1, 1, "", this)
        };

        private Among a_7[] = {
            new Among ( "isca", -1, 1, "", this),
            new Among ( "enda", -1, 1, "", this),
            new Among ( "ata", -1, 1, "", this),
            new Among ( "ita", -1, 1, "", this),
            new Among ( "uta", -1, 1, "", this),
            new Among ( "ava", -1, 1, "", this),
            new Among ( "eva", -1, 1, "", this),
            new Among ( "iva", -1, 1, "", this),
            new Among ( "erebbe", -1, 1, "", this),
            new Among ( "irebbe", -1, 1, "", this),
            new Among ( "isce", -1, 1, "", this),
            new Among ( "ende", -1, 1, "", this),
            new Among ( "are", -1, 1, "", this),
            new Among ( "ere", -1, 1, "", this),
            new Among ( "ire", -1, 1, "", this),
            new Among ( "asse", -1, 1, "", this),
            new Among ( "ate", -1, 1, "", this),
            new Among ( "avate", 16, 1, "", this),
            new Among ( "evate", 16, 1, "", this),
            new Among ( "ivate", 16, 1, "", this),
            new Among ( "ete", -1, 1, "", this),
            new Among ( "erete", 20, 1, "", this),
            new Among ( "irete", 20, 1, "", this),
            new Among ( "ite", -1, 1, "", this),
            new Among ( "ereste", -1, 1, "", this),
            new Among ( "ireste", -1, 1, "", this),
            new Among ( "ute", -1, 1, "", this),
            new Among ( "erai", -1, 1, "", this),
            new Among ( "irai", -1, 1, "", this),
            new Among ( "isci", -1, 1, "", this),
            new Among ( "endi", -1, 1, "", this),
            new Among ( "erei", -1, 1, "", this),
            new Among ( "irei", -1, 1, "", this),
            new Among ( "assi", -1, 1, "", this),
            new Among ( "ati", -1, 1, "", this),
            new Among ( "iti", -1, 1, "", this),
            new Among ( "eresti", -1, 1, "", this),
            new Among ( "iresti", -1, 1, "", this),
            new Among ( "uti", -1, 1, "", this),
            new Among ( "avi", -1, 1, "", this),
            new Among ( "evi", -1, 1, "", this),
            new Among ( "ivi", -1, 1, "", this),
            new Among ( "isco", -1, 1, "", this),
            new Among ( "ando", -1, 1, "", this),
            new Among ( "endo", -1, 1, "", this),
            new Among ( "Yamo", -1, 1, "", this),
            new Among ( "iamo", -1, 1, "", this),
            new Among ( "avamo", -1, 1, "", this),
            new Among ( "evamo", -1, 1, "", this),
            new Among ( "ivamo", -1, 1, "", this),
            new Among ( "eremo", -1, 1, "", this),
            new Among ( "iremo", -1, 1, "", this),
            new Among ( "assimo", -1, 1, "", this),
            new Among ( "ammo", -1, 1, "", this),
            new Among ( "emmo", -1, 1, "", this),
            new Among ( "eremmo", 54, 1, "", this),
            new Among ( "iremmo", 54, 1, "", this),
            new Among ( "immo", -1, 1, "", this),
            new Among ( "ano", -1, 1, "", this),
            new Among ( "iscano", 58, 1, "", this),
            new Among ( "avano", 58, 1, "", this),
            new Among ( "evano", 58, 1, "", this),
            new Among ( "ivano", 58, 1, "", this),
            new Among ( "eranno", -1, 1, "", this),
            new Among ( "iranno", -1, 1, "", this),
            new Among ( "ono", -1, 1, "", this),
            new Among ( "iscono", 65, 1, "", this),
            new Among ( "arono", 65, 1, "", this),
            new Among ( "erono", 65, 1, "", this),
            new Among ( "irono", 65, 1, "", this),
            new Among ( "erebbero", -1, 1, "", this),
            new Among ( "irebbero", -1, 1, "", this),
            new Among ( "assero", -1, 1, "", this),
            new Among ( "essero", -1, 1, "", this),
            new Among ( "issero", -1, 1, "", this),
            new Among ( "ato", -1, 1, "", this),
            new Among ( "ito", -1, 1, "", this),
            new Among ( "uto", -1, 1, "", this),
            new Among ( "avo", -1, 1, "", this),
            new Among ( "evo", -1, 1, "", this),
            new Among ( "ivo", -1, 1, "", this),
            new Among ( "ar", -1, 1, "", this),
            new Among ( "ir", -1, 1, "", this),
            new Among ( "er\u00E0", -1, 1, "", this),
            new Among ( "ir\u00E0", -1, 1, "", this),
            new Among ( "er\u00F2", -1, 1, "", this),
            new Among ( "ir\u00F2", -1, 1, "", this)
        };

        private static final char g_v[] = {17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 128, 8, 2, 1 };

        private static final char g_AEIO[] = {17, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 128, 8, 2 };

        private static final char g_CG[] = {17 };

        private int I_p2;
        private int I_p1;
        private int I_pV;

        private void copy_from(ItalianStemmer other) {
            I_p2 = other.I_p2;
            I_p1 = other.I_p1;
            I_pV = other.I_pV;
            super.copy_from(other);
        }

        private boolean r_prelude() {
            int among_var;
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            // (, line 34
            // test, line 35
            v_1 = cursor;
            // repeat, line 35
            replab0: while(true)
            {
                v_2 = cursor;
                lab1: do {
                    // (, line 35
                    // [, line 36
                    bra = cursor;
                    // substring, line 36
                    among_var = find_among(a_0, 7);
                    if (among_var == 0)
                    {
                        break lab1;
                    }
                    // ], line 36
                    ket = cursor;
                    switch(among_var) {
                        case 0:
                            break lab1;
                        case 1:
                            // (, line 37
                            // <-, line 37
                            slice_from("\u00E0");
                            break;
                        case 2:
                            // (, line 38
                            // <-, line 38
                            slice_from("\u00E8");
                            break;
                        case 3:
                            // (, line 39
                            // <-, line 39
                            slice_from("\u00EC");
                            break;
                        case 4:
                            // (, line 40
                            // <-, line 40
                            slice_from("\u00F2");
                            break;
                        case 5:
                            // (, line 41
                            // <-, line 41
                            slice_from("\u00F9");
                            break;
                        case 6:
                            // (, line 42
                            // <-, line 42
                            slice_from("qU");
                            break;
                        case 7:
                            // (, line 43
                            // next, line 43
                            if (cursor >= limit)
                            {
                                break lab1;
                            }
                            cursor++;
                            break;
                    }
                    continue replab0;
                } while (false);
                cursor = v_2;
                break replab0;
            }
            cursor = v_1;
            // repeat, line 46
            replab2: while(true)
            {
                v_3 = cursor;
                lab3: do {
                    // goto, line 46
                    golab4: while(true)
                    {
                        v_4 = cursor;
                        lab5: do {
                            // (, line 46
                            if (!(in_grouping(g_v, 97, 249)))
                            {
                                break lab5;
                            }
                            // [, line 47
                            bra = cursor;
                            // or, line 47
                            lab6: do {
                                v_5 = cursor;
                                lab7: do {
                                    // (, line 47
                                    // literal, line 47
                                    if (!(eq_s(1, "u")))
                                    {
                                        break lab7;
                                    }
                                    // ], line 47
                                    ket = cursor;
                                    if (!(in_grouping(g_v, 97, 249)))
                                    {
                                        break lab7;
                                    }
                                    // <-, line 47
                                    slice_from("U");
                                    break lab6;
                                } while (false);
                                cursor = v_5;
                                // (, line 48
                                // literal, line 48
                                if (!(eq_s(1, "i")))
                                {
                                    break lab5;
                                }
                                // ], line 48
                                ket = cursor;
                                if (!(in_grouping(g_v, 97, 249)))
                                {
                                    break lab5;
                                }
                                // <-, line 48
                                slice_from("I");
                            } while (false);
                            cursor = v_4;
                            break golab4;
                        } while (false);
                        cursor = v_4;
                        if (cursor >= limit)
                        {
                            break lab3;
                        }
                        cursor++;
                    }
                    continue replab2;
                } while (false);
                cursor = v_3;
                break replab2;
            }
            return true;
        }

        private boolean r_mark_regions() {
            int v_1;
            int v_2;
            int v_3;
            int v_6;
            int v_8;
            // (, line 52
            I_pV = limit;
            I_p1 = limit;
            I_p2 = limit;
            // do, line 58
            v_1 = cursor;
            lab0: do {
                // (, line 58
                // or, line 60
                lab1: do {
                    v_2 = cursor;
                    lab2: do {
                        // (, line 59
                        if (!(in_grouping(g_v, 97, 249)))
                        {
                            break lab2;
                        }
                        // or, line 59
                        lab3: do {
                            v_3 = cursor;
                            lab4: do {
                                // (, line 59
                                if (!(out_grouping(g_v, 97, 249)))
                                {
                                    break lab4;
                                }
                                // gopast, line 59
                                golab5: while(true)
                                {
                                    lab6: do {
                                        if (!(in_grouping(g_v, 97, 249)))
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
                            // (, line 59
                            if (!(in_grouping(g_v, 97, 249)))
                            {
                                break lab2;
                            }
                            // gopast, line 59
                            golab7: while(true)
                            {
                                lab8: do {
                                    if (!(out_grouping(g_v, 97, 249)))
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
                    // (, line 61
                    if (!(out_grouping(g_v, 97, 249)))
                    {
                        break lab0;
                    }
                    // or, line 61
                    lab9: do {
                        v_6 = cursor;
                        lab10: do {
                            // (, line 61
                            if (!(out_grouping(g_v, 97, 249)))
                            {
                                break lab10;
                            }
                            // gopast, line 61
                            golab11: while(true)
                            {
                                lab12: do {
                                    if (!(in_grouping(g_v, 97, 249)))
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
                        // (, line 61
                        if (!(in_grouping(g_v, 97, 249)))
                        {
                            break lab0;
                        }
                        // next, line 61
                        if (cursor >= limit)
                        {
                            break lab0;
                        }
                        cursor++;
                    } while (false);
                } while (false);
                // setmark pV, line 62
                I_pV = cursor;
            } while (false);
            cursor = v_1;
            // do, line 64
            v_8 = cursor;
            lab13: do {
                // (, line 64
                // gopast, line 65
                golab14: while(true)
                {
                    lab15: do {
                        if (!(in_grouping(g_v, 97, 249)))
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
                // gopast, line 65
                golab16: while(true)
                {
                    lab17: do {
                        if (!(out_grouping(g_v, 97, 249)))
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
                // setmark p1, line 65
                I_p1 = cursor;
                // gopast, line 66
                golab18: while(true)
                {
                    lab19: do {
                        if (!(in_grouping(g_v, 97, 249)))
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
                // gopast, line 66
                golab20: while(true)
                {
                    lab21: do {
                        if (!(out_grouping(g_v, 97, 249)))
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
                // setmark p2, line 66
                I_p2 = cursor;
            } while (false);
            cursor = v_8;
            return true;
        }

        private boolean r_postlude() {
            int among_var;
            int v_1;
            // repeat, line 70
            replab0: while(true)
            {
                v_1 = cursor;
                lab1: do {
                    // (, line 70
                    // [, line 72
                    bra = cursor;
                    // substring, line 72
                    among_var = find_among(a_1, 3);
                    if (among_var == 0)
                    {
                        break lab1;
                    }
                    // ], line 72
                    ket = cursor;
                    switch(among_var) {
                        case 0:
                            break lab1;
                        case 1:
                            // (, line 73
                            // <-, line 73
                            slice_from("i");
                            break;
                        case 2:
                            // (, line 74
                            // <-, line 74
                            slice_from("u");
                            break;
                        case 3:
                            // (, line 75
                            // next, line 75
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
            // (, line 86
            // [, line 87
            ket = cursor;
            // substring, line 87
            if (find_among_b(a_2, 37) == 0)
            {
                return false;
            }
            // ], line 87
            bra = cursor;
            // among, line 97
            among_var = find_among_b(a_3, 5);
            if (among_var == 0)
            {
                return false;
            }
            // (, line 97
            // call RV, line 97
            if (!r_RV())
            {
                return false;
            }
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 98
                    // delete, line 98
                    slice_del();
                    break;
                case 2:
                    // (, line 99
                    // <-, line 99
                    slice_from("e");
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
            // (, line 103
            // [, line 104
            ket = cursor;
            // substring, line 104
            among_var = find_among_b(a_6, 49);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 104
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 110
                    // call R2, line 110
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 110
                    slice_del();
                    break;
                case 2:
                    // (, line 112
                    // call R2, line 112
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 112
                    slice_del();
                    // try, line 113
                    v_1 = limit - cursor;
                    lab0: do {
                        // (, line 113
                        // [, line 113
                        ket = cursor;
                        // literal, line 113
                        if (!(eq_s_b(2, "ic")))
                        {
                            cursor = limit - v_1;
                            break lab0;
                        }
                        // ], line 113
                        bra = cursor;
                        // call R2, line 113
                        if (!r_R2())
                        {
                            cursor = limit - v_1;
                            break lab0;
                        }
                        // delete, line 113
                        slice_del();
                    } while (false);
                    break;
                case 3:
                    // (, line 116
                    // call R2, line 116
                    if (!r_R2())
                    {
                        return false;
                    }
                    // <-, line 116
                    slice_from("log");
                    break;
                case 4:
                    // (, line 118
                    // call R2, line 118
                    if (!r_R2())
                    {
                        return false;
                    }
                    // <-, line 118
                    slice_from("u");
                    break;
                case 5:
                    // (, line 120
                    // call R2, line 120
                    if (!r_R2())
                    {
                        return false;
                    }
                    // <-, line 120
                    slice_from("ente");
                    break;
                case 6:
                    // (, line 122
                    // call RV, line 122
                    if (!r_RV())
                    {
                        return false;
                    }
                    // delete, line 122
                    slice_del();
                    break;
                case 7:
                    // (, line 123
                    // call R1, line 124
                    if (!r_R1())
                    {
                        return false;
                    }
                    // delete, line 124
                    slice_del();
                    // try, line 125
                    v_2 = limit - cursor;
                    lab1: do {
                        // (, line 125
                        // [, line 126
                        ket = cursor;
                        // substring, line 126
                        among_var = find_among_b(a_4, 4);
                        if (among_var == 0)
                        {
                            cursor = limit - v_2;
                            break lab1;
                        }
                        // ], line 126
                        bra = cursor;
                        // call R2, line 126
                        if (!r_R2())
                        {
                            cursor = limit - v_2;
                            break lab1;
                        }
                        // delete, line 126
                        slice_del();
                        switch(among_var) {
                            case 0:
                                cursor = limit - v_2;
                                break lab1;
                            case 1:
                                // (, line 127
                                // [, line 127
                                ket = cursor;
                                // literal, line 127
                                if (!(eq_s_b(2, "at")))
                                {
                                    cursor = limit - v_2;
                                    break lab1;
                                }
                                // ], line 127
                                bra = cursor;
                                // call R2, line 127
                                if (!r_R2())
                                {
                                    cursor = limit - v_2;
                                    break lab1;
                                }
                                // delete, line 127
                                slice_del();
                                break;
                        }
                    } while (false);
                    break;
                case 8:
                    // (, line 132
                    // call R2, line 133
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 133
                    slice_del();
                    // try, line 134
                    v_3 = limit - cursor;
                    lab2: do {
                        // (, line 134
                        // [, line 135
                        ket = cursor;
                        // substring, line 135
                        among_var = find_among_b(a_5, 3);
                        if (among_var == 0)
                        {
                            cursor = limit - v_3;
                            break lab2;
                        }
                        // ], line 135
                        bra = cursor;
                        switch(among_var) {
                            case 0:
                                cursor = limit - v_3;
                                break lab2;
                            case 1:
                                // (, line 136
                                // call R2, line 136
                                if (!r_R2())
                                {
                                    cursor = limit - v_3;
                                    break lab2;
                                }
                                // delete, line 136
                                slice_del();
                                break;
                        }
                    } while (false);
                    break;
                case 9:
                    // (, line 140
                    // call R2, line 141
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 141
                    slice_del();
                    // try, line 142
                    v_4 = limit - cursor;
                    lab3: do {
                        // (, line 142
                        // [, line 142
                        ket = cursor;
                        // literal, line 142
                        if (!(eq_s_b(2, "at")))
                        {
                            cursor = limit - v_4;
                            break lab3;
                        }
                        // ], line 142
                        bra = cursor;
                        // call R2, line 142
                        if (!r_R2())
                        {
                            cursor = limit - v_4;
                            break lab3;
                        }
                        // delete, line 142
                        slice_del();
                        // [, line 142
                        ket = cursor;
                        // literal, line 142
                        if (!(eq_s_b(2, "ic")))
                        {
                            cursor = limit - v_4;
                            break lab3;
                        }
                        // ], line 142
                        bra = cursor;
                        // call R2, line 142
                        if (!r_R2())
                        {
                            cursor = limit - v_4;
                            break lab3;
                        }
                        // delete, line 142
                        slice_del();
                    } while (false);
                    break;
            }
            return true;
        }

        private boolean r_verb_suffix() {
            int among_var;
            int v_1;
            int v_2;
            // setlimit, line 147
            v_1 = limit - cursor;
            // tomark, line 147
            if (cursor < I_pV)
            {
                return false;
            }
            cursor = I_pV;
            v_2 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_1;
            // (, line 147
            // [, line 148
            ket = cursor;
            // substring, line 148
            among_var = find_among_b(a_7, 87);
            if (among_var == 0)
            {
                limit_backward = v_2;
                return false;
            }
            // ], line 148
            bra = cursor;
            switch(among_var) {
                case 0:
                    limit_backward = v_2;
                    return false;
                case 1:
                    // (, line 162
                    // delete, line 162
                    slice_del();
                    break;
            }
            limit_backward = v_2;
            return true;
        }

        private boolean r_vowel_suffix() {
            int v_1;
            int v_2;
            // (, line 169
            // try, line 170
            v_1 = limit - cursor;
            lab0: do {
                // (, line 170
                // [, line 171
                ket = cursor;
                if (!(in_grouping_b(g_AEIO, 97, 242)))
                {
                    cursor = limit - v_1;
                    break lab0;
                }
                // ], line 171
                bra = cursor;
                // call RV, line 171
                if (!r_RV())
                {
                    cursor = limit - v_1;
                    break lab0;
                }
                // delete, line 171
                slice_del();
                // [, line 172
                ket = cursor;
                // literal, line 172
                if (!(eq_s_b(1, "i")))
                {
                    cursor = limit - v_1;
                    break lab0;
                }
                // ], line 172
                bra = cursor;
                // call RV, line 172
                if (!r_RV())
                {
                    cursor = limit - v_1;
                    break lab0;
                }
                // delete, line 172
                slice_del();
            } while (false);
            // try, line 174
            v_2 = limit - cursor;
            lab1: do {
                // (, line 174
                // [, line 175
                ket = cursor;
                // literal, line 175
                if (!(eq_s_b(1, "h")))
                {
                    cursor = limit - v_2;
                    break lab1;
                }
                // ], line 175
                bra = cursor;
                if (!(in_grouping_b(g_CG, 99, 103)))
                {
                    cursor = limit - v_2;
                    break lab1;
                }
                // call RV, line 175
                if (!r_RV())
                {
                    cursor = limit - v_2;
                    break lab1;
                }
                // delete, line 175
                slice_del();
            } while (false);
            return true;
        }

        public boolean stem() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            // (, line 180
            // do, line 181
            v_1 = cursor;
            lab0: do {
                // call prelude, line 181
                if (!r_prelude())
                {
                    break lab0;
                }
            } while (false);
            cursor = v_1;
            // do, line 182
            v_2 = cursor;
            lab1: do {
                // call mark_regions, line 182
                if (!r_mark_regions())
                {
                    break lab1;
                }
            } while (false);
            cursor = v_2;
            // backwards, line 183
            limit_backward = cursor; cursor = limit;
            // (, line 183
            // do, line 184
            v_3 = limit - cursor;
            lab2: do {
                // call attached_pronoun, line 184
                if (!r_attached_pronoun())
                {
                    break lab2;
                }
            } while (false);
            cursor = limit - v_3;
            // do, line 185
            v_4 = limit - cursor;
            lab3: do {
                // (, line 185
                // or, line 185
                lab4: do {
                    v_5 = limit - cursor;
                    lab5: do {
                        // call standard_suffix, line 185
                        if (!r_standard_suffix())
                        {
                            break lab5;
                        }
                        break lab4;
                    } while (false);
                    cursor = limit - v_5;
                    // call verb_suffix, line 185
                    if (!r_verb_suffix())
                    {
                        break lab3;
                    }
                } while (false);
            } while (false);
            cursor = limit - v_4;
            // do, line 186
            v_6 = limit - cursor;
            lab6: do {
                // call vowel_suffix, line 186
                if (!r_vowel_suffix())
                {
                    break lab6;
                }
            } while (false);
            cursor = limit - v_6;
            cursor = limit_backward;            // do, line 188
            v_7 = cursor;
            lab7: do {
                // call postlude, line 188
                if (!r_postlude())
                {
                    break lab7;
                }
            } while (false);
            cursor = v_7;
            return true;
        }

}

