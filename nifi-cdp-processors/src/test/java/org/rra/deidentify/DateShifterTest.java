package org.rra.deidentify;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateShifterTest {

    @Test
    public void shiftsDaAcrossMonthYearAndLeapDay() {
        Assertions.assertEquals("20200210", DateShifter.shiftDA("20200131", 10));
        Assertions.assertEquals("20191231", DateShifter.shiftDA("20200101", -1));
        Assertions.assertEquals("20200229", DateShifter.shiftDA("20200228", 1));
        Assertions.assertEquals("20210301", DateShifter.shiftDA("20210228", 1));
        Assertions.assertEquals("20191210", DateShifter.shiftDA("20200229", -81));
    }

    @Test
    public void shiftsOldDotFormatAndRanges() {
        Assertions.assertEquals("20200110", DateShifter.shiftDA("2020.01.01", 9));
        Assertions.assertEquals("20200111-20200211", DateShifter.shiftDA("20200101-20200201", 10));
        Assertions.assertEquals("20200111-", DateShifter.shiftDA("20200101-", 10));
        Assertions.assertEquals("-20200211", DateShifter.shiftDA("-20200201", 10));
    }

    @Test
    public void clearsWhatCannotBeShifted() {
        Assertions.assertEquals("", DateShifter.shiftDA("20200230", 5));
        Assertions.assertEquals("", DateShifter.shiftDA("2020", 5));
        Assertions.assertEquals("", DateShifter.shiftDA("abcdefgh", 5));
        Assertions.assertEquals("", DateShifter.shiftDA("  ", 5));
        Assertions.assertEquals("", DateShifter.shiftDT("2020", 5));
        Assertions.assertEquals("", DateShifter.shiftDT("202003", 5));
    }

    @Test
    public void shiftsOnlyTheDatePartOfDt() {
        Assertions.assertEquals("20191231233059.123456+0100", DateShifter.shiftDT("20200101233059.123456+0100", -1));
        Assertions.assertEquals("20200111", DateShifter.shiftDT("20200101", 10));
        Assertions.assertEquals("2020011112", DateShifter.shiftDT("2020010112", 10));
        Assertions.assertEquals("20200111-0500", DateShifter.shiftDT("20200101-0500", 10));
    }

    @Test
    public void shiftsMultiValuedAndNestedValuesButNotTimes() {
        Attributes attrs = new Attributes();
        attrs.setString(Tag.StudyDate, VR.DA, "20200101");
        attrs.setString(Tag.StudyTime, VR.TM, "101500");
        attrs.setString(Tag.CalibrationDate, VR.DA, "20200101", "20200105");
        attrs.setString(Tag.AcquisitionDateTime, VR.DT, "20200101101500.5");
        Sequence frames = attrs.newSequence(Tag.PerFrameFunctionalGroupsSequence, 1);
        Attributes frame = new Attributes();
        Sequence content = frame.newSequence(Tag.FrameContentSequence, 1);
        Attributes contentItem = new Attributes();
        contentItem.setString(Tag.FrameAcquisitionDateTime, VR.DT, "20200101101501");
        content.add(contentItem);
        frames.add(frame);
        Sequence sr = attrs.newSequence(Tag.ContentSequence, 1);
        Attributes srItem = new Attributes();
        srItem.setString(Tag.Date, VR.DA, "20200301");
        sr.add(srItem);

        DateShifter.shift(attrs, -42);

        Assertions.assertEquals("20191120", attrs.getString(Tag.StudyDate));
        Assertions.assertEquals("101500", attrs.getString(Tag.StudyTime));
        Assertions.assertArrayEquals(new String[]{"20191120", "20191124"}, attrs.getStrings(Tag.CalibrationDate));
        Assertions.assertEquals("20191120101500.5", attrs.getString(Tag.AcquisitionDateTime));
        Assertions.assertEquals("20191120101501", attrs.getSequence(Tag.PerFrameFunctionalGroupsSequence).get(0)
                .getSequence(Tag.FrameContentSequence).get(0).getString(Tag.FrameAcquisitionDateTime));
        Assertions.assertEquals("20200119", attrs.getSequence(Tag.ContentSequence).get(0).getString(Tag.Date));
    }

    @Test
    public void zeroChangesNothingAndRangeIsChecked() {
        Attributes attrs = new Attributes();
        attrs.setString(Tag.StudyDate, VR.DA, "20200101");
        DateShifter.shift(attrs, 0);
        Assertions.assertEquals("20200101", attrs.getString(Tag.StudyDate));
        DateShifter.shift(attrs, DateShifter.MAX_DAYS);
        Assertions.assertThrows(IllegalArgumentException.class, () -> DateShifter.shift(attrs, DateShifter.MAX_DAYS + 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> DateShifter.shift(attrs, -DateShifter.MAX_DAYS - 1));
    }
}
