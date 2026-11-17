package org.rra.deidentify;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.VR;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Shifts every DA and DT value of a data set by a number of days, including
 * the values in nested sequences (DICOM PS3.15, Retain Longitudinal Temporal
 * Information with Modified Dates Option).
 *
 * <ul>
 *   <li>DA: {@code YYYYMMDD} (also the old {@code YYYY.MM.DD}), multi-valued and
 *       ranges ({@code A-B}) are shifted value by value.</li>
 *   <li>DT: the date part (first 8 characters) is shifted; time, fraction and
 *       UTC offset stay as they are.</li>
 *   <li>Times (TM) are not changed.</li>
 *   <li>A value that cannot be shifted to the day (invalid date, DT with year or
 *       month only) is cleared, so that no real date is left in the data set.</li>
 * </ul>
 */
public final class DateShifter {

    /** Largest accepted shift in days, about 100 years. */
    public static final int MAX_DAYS = 36500;

    private static final DateTimeFormatter DA = DateTimeFormatter.BASIC_ISO_DATE;

    private DateShifter() {
    }

    /**
     * @param attrs data set, changed in place
     * @param days  shift in days; 0 changes nothing
     * @throws IllegalArgumentException if {@code |days| > MAX_DAYS}
     */
    public static void shift(Attributes attrs, int days) {
        checkRange(days);
        if (days == 0 || attrs == null) {
            return;
        }
        shiftItem(attrs, days);
    }

    public static void checkRange(int days) {
        if (days < -MAX_DAYS || days > MAX_DAYS) {
            throw new IllegalArgumentException("Date shift out of range: " + days);
        }
    }

    private static void shiftItem(Attributes attrs, int days) {
        for (int tag : attrs.tags()) {
            VR vr = attrs.getVR(tag);
            if (vr == VR.SQ) {
                Sequence sq = attrs.getSequence(tag);
                if (sq != null) {
                    for (Attributes item : sq) {
                        shiftItem(item, days);
                    }
                }
            } else if (vr == VR.DA || vr == VR.DT) {
                String[] values = attrs.getStrings(tag);
                if (values == null || values.length == 0) {
                    continue;
                }
                String[] shifted = new String[values.length];
                for (int i = 0; i < values.length; i++) {
                    shifted[i] = vr == VR.DA ? shiftDA(values[i], days) : shiftDT(values[i], days);
                }
                attrs.setString(tag, vr, shifted);
            }
        }
    }

    /** Shifts one DA value (or DA range); returns "" if it cannot be shifted. */
    static String shiftDA(String value, int days) {
        if (value == null) {
            return "";
        }
        String v = value.trim();
        if (v.isEmpty()) {
            return "";
        }
        int dash = v.indexOf('-');
        if (dash >= 0) {
            String from = v.substring(0, dash).trim();
            String to = v.substring(dash + 1).trim();
            String shiftedFrom = from.isEmpty() ? "" : shiftDA(from, days);
            String shiftedTo = to.isEmpty() ? "" : shiftDA(to, days);
            if ((!from.isEmpty() && shiftedFrom.isEmpty()) || (!to.isEmpty() && shiftedTo.isEmpty())
                    || (from.isEmpty() && to.isEmpty())) {
                return "";
            }
            return shiftedFrom + "-" + shiftedTo;
        }
        if (v.length() == 10 && v.charAt(4) == '.' && v.charAt(7) == '.') {
            v = v.substring(0, 4) + v.substring(5, 7) + v.substring(8);
        }
        if (v.length() != 8) {
            return "";
        }
        try {
            return LocalDate.parse(v, DA).plusDays(days).format(DA);
        } catch (DateTimeParseException e) {
            return "";
        }
    }

    /** Shifts the date part of one DT value; returns "" if it cannot be shifted. */
    static String shiftDT(String value, int days) {
        if (value == null) {
            return "";
        }
        String v = value.trim();
        if (v.length() < 8) {
            return "";
        }
        String date = shiftDA(v.substring(0, 8), days);
        return date.isEmpty() ? "" : date + v.substring(8);
    }
}
