package org.rra.deidentify;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeidentifyDateShiftTest {

    private static final int SHIFT = -42;
    private static final String[] DICOM_DIRS = {
            "src/test/resources/data/DICOM/ct_IVRLE/",
            "src/test/resources/data/DICOM/mr_rra/",
            "src/test/resources/data/DICOM/sr/",
            "src/test/resources/data/DICOM/rt/",
            "src/test/resources/data/DICOM/pdfdcm/",
    };

    private static Attributes sample() {
        Attributes attrs = new Attributes();
        attrs.setString(Tag.SOPClassUID, VR.UI, "1.2.840.10008.5.1.4.1.1.2");
        attrs.setString(Tag.StudyInstanceUID, VR.UI, "1.2.3.4");
        attrs.setString(Tag.SeriesInstanceUID, VR.UI, "1.2.3.4.5");
        attrs.setString(Tag.SOPInstanceUID, VR.UI, "1.2.3.4.5.6");
        attrs.setString(Tag.PatientID, VR.LO, "0001234567");
        attrs.setString(Tag.PatientName, VR.PN, "Muster^Hans");
        attrs.setString(Tag.PatientBirthDate, VR.DA, "19700615");
        attrs.setString(Tag.PatientAge, VR.AS, "049Y");
        attrs.setString(Tag.StudyDate, VR.DA, "20200101");
        attrs.setString(Tag.StudyTime, VR.TM, "101500");
        attrs.setString(Tag.SeriesDate, VR.DA, "20200101");
        attrs.setString(Tag.InstanceCreationDate, VR.DA, "20200102");
        attrs.setString(Tag.ContentDate, VR.DA, "20200101");
        attrs.setString(Tag.AcquisitionDateTime, VR.DT, "20200101101530.25+0100");
        attrs.setString(Tag.TimezoneOffsetFromUTC, VR.SH, "+0100");
        attrs.setString(Tag.DeviceLabel, VR.LO, "Scanner 3");
        Sequence frames = attrs.newSequence(Tag.PerFrameFunctionalGroupsSequence, 1);
        Attributes frame = new Attributes();
        Attributes content = new Attributes();
        content.setString(Tag.FrameAcquisitionDateTime, VR.DT, "20200105080000");
        frame.newSequence(Tag.FrameContentSequence, 1).add(content);
        frames.add(frame);
        return attrs;
    }

    @Test
    public void withShiftAllDatesAreShiftedAndMarked() {
        Attributes out = new Deidentify().deidentifyAttributes(sample(), "", "PS-1", "PS^1", SHIFT);

        Assertions.assertEquals("20191120", out.getString(Tag.StudyDate));
        Assertions.assertEquals("20191120", out.getString(Tag.SeriesDate));
        Assertions.assertEquals("20191121", out.getString(Tag.InstanceCreationDate));
        Assertions.assertEquals("20191120", out.getString(Tag.ContentDate));
        Assertions.assertEquals("20191120101530.25+0100", out.getString(Tag.AcquisitionDateTime));
        // PS3.15: birth date Z, age X, also with Modified Dates
        Assertions.assertNull(out.getString(Tag.PatientBirthDate));
        Assertions.assertFalse(out.contains(Tag.PatientAge));
        Assertions.assertEquals("20191124080000", out.getSequence(Tag.PerFrameFunctionalGroupsSequence).get(0)
                .getSequence(Tag.FrameContentSequence).get(0).getString(Tag.FrameAcquisitionDateTime));
        // times stay
        Assertions.assertEquals("101500", out.getString(Tag.StudyTime));
        // not dates: still removed or replaced
        Assertions.assertFalse(out.contains(Tag.TimezoneOffsetFromUTC));
        Assertions.assertNotEquals("Scanner 3", out.getString(Tag.DeviceLabel));
        // identity
        Assertions.assertEquals("PS-1", out.getString(Tag.PatientID));
        Assertions.assertEquals("PS^1", out.getString(Tag.PatientName));
        Assertions.assertNotEquals("1.2.3.4", out.getString(Tag.StudyInstanceUID));
        // marking
        Assertions.assertEquals("MODIFIED", out.getString(Tag.LongitudinalTemporalInformationModified));
        Assertions.assertTrue(codes(out).contains("113107"));
    }

    @Test
    public void withoutShiftDatesAreRemovedAsBefore() {
        for (Integer shift : new Integer[]{0, null}) {
            Attributes out = new Deidentify().deidentifyAttributes(sample(), "", "PS-1", "PS^1", shift);

            Assertions.assertNull(out.getString(Tag.StudyDate));
            Assertions.assertNull(out.getString(Tag.PatientBirthDate));
            Assertions.assertFalse(out.contains(Tag.PatientAge));
            Assertions.assertFalse(out.contains(Tag.InstanceCreationDate));
            Assertions.assertEquals("19991111", out.getString(Tag.ContentDate));
            Assertions.assertEquals("REMOVED", out.getString(Tag.LongitudinalTemporalInformationModified));
            Assertions.assertFalse(codes(out).contains("113107"));
        }
    }

    @Test
    public void retainedDatesAreShiftedToo() {
        Attributes out = new Deidentify().deidentifyAttributes(sample(), "StudyDate", "PS-1", "PS^1", SHIFT);
        Assertions.assertEquals("20191120", out.getString(Tag.StudyDate));
    }

    @Test
    public void ageIsKeptOnlyWhenRetained() {
        Attributes out = new Deidentify().deidentifyAttributes(sample(), "PatientAge", "PS-1", "PS^1", SHIFT);
        Assertions.assertEquals("049Y", out.getString(Tag.PatientAge));
        Assertions.assertNull(out.getString(Tag.PatientBirthDate));
    }

    @Test
    public void shiftOutOfRangeIsRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new Deidentify().deidentifyAttributes(sample(), "", "PS-1", "PS^1", DateShifter.MAX_DAYS + 1));
    }

    /**
     * Real CT, MR, SR, RT and PDF objects: every date left in the result is an
     * original date moved by the shift; no original or dummy date remains.
     */
    @Test
    public void realObjectsContainOnlyShiftedDates() throws IOException {
        int files = 0;
        for (String dir : DICOM_DIRS) {
            Path root = Paths.get(dir);
            if (!Files.isDirectory(root)) {
                continue;
            }
            List<File> list;
            try (Stream<Path> walk = Files.walk(root)) {
                list = walk.filter(Files::isRegularFile).map(Path::toFile).collect(Collectors.toList());
            }
            for (File file : list) {
                Attributes original = read(file);
                if (original == null) {
                    continue;
                }
                Set<String> originalDates = new HashSet<>();
                collectDates(original, originalDates);
                Attributes out = new Deidentify().deidentifyAttributes(new Attributes(original), "", "PS-1", "PS^1", SHIFT);
                List<String> shifted = new ArrayList<>();
                collectDates(out, shifted);
                for (String date : shifted) {
                    String back = LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE).minusDays(SHIFT)
                            .format(DateTimeFormatter.BASIC_ISO_DATE);
                    Assertions.assertTrue(originalDates.contains(back),
                            file + ": " + date + " is not a shifted original date");
                }
                String studyDate = original.getString(Tag.StudyDate);
                if (studyDate != null && studyDate.length() == 8) {
                    Assertions.assertEquals(DateShifter.shiftDA(studyDate, SHIFT), out.getString(Tag.StudyDate), file.toString());
                }
                Assertions.assertEquals("MODIFIED", out.getString(Tag.LongitudinalTemporalInformationModified));
                files++;
            }
        }
        Assertions.assertTrue(files > 10, "too few test objects found: " + files);
    }

    private static Attributes read(File file) {
        try (DicomInputStream in = new DicomInputStream(file)) {
            return in.readDataset();
        } catch (IOException e) {
            return null;
        }
    }

    /** Date part (YYYYMMDD) of every DA and DT value, also in sequences. */
    private static void collectDates(Attributes attrs, java.util.Collection<String> dates) {
        for (int tag : attrs.tags()) {
            VR vr = attrs.getVR(tag);
            if (vr == VR.SQ) {
                Sequence sq = attrs.getSequence(tag);
                if (sq != null) {
                    for (Attributes item : sq) {
                        collectDates(item, dates);
                    }
                }
            } else if (vr == VR.DA || vr == VR.DT) {
                String[] values = attrs.getStrings(tag);
                if (values == null) {
                    continue;
                }
                for (String v : values) {
                    if (v != null && v.trim().length() >= 8) {
                        dates.add(v.trim().substring(0, 8).replace(".", ""));
                    }
                }
            }
        }
    }

    private static Set<String> codes(Attributes attrs) {
        Set<String> codes = new HashSet<>();
        Sequence sq = attrs.getSequence(Tag.DeidentificationMethodCodeSequence);
        if (sq != null) {
            for (Attributes item : sq) {
                codes.add(item.getString(Tag.CodeValue));
            }
        }
        return codes;
    }
}
