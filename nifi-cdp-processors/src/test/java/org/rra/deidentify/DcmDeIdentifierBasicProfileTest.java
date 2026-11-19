package org.rra.deidentify;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Actions of DICOM PS3.15 Table E.1-1 (2026b) that the tag tables once got wrong. */
public class DcmDeIdentifierBasicProfileTest {

    private static Attributes deidentify(Attributes attrs, DcmDeIdentifier.Option... options) {
        new DcmDeIdentifier(options).deidentify(attrs, Collections.emptyList());
        return attrs;
    }

    @Test
    public void contentItemDateTimeIsReplacedNotRemoved() {
        Attributes attrs = new Attributes();
        attrs.setString(Tag.DateTime, VR.DT, "20240101120000");
        deidentify(attrs);
        Assertions.assertTrue(attrs.contains(Tag.DateTime), "D: DateTime must stay present");
        Assertions.assertNotEquals("20240101120000", attrs.getString(Tag.DateTime));
    }

    @Test
    public void contentItemDateTimeIsKeptForDateShift() {
        Attributes attrs = new Attributes();
        attrs.setString(Tag.DateTime, VR.DT, "20240101120000");
        deidentify(attrs, DcmDeIdentifier.Option.RetainLongitudinalTemporalInformationModifiedDatesOption);
        Assertions.assertEquals("20240101120000", attrs.getString(Tag.DateTime));
    }

    @Test
    public void digitalSignatureUidIsRemapped() {
        Attributes attrs = new Attributes();
        attrs.setString(Tag.DigitalSignatureUID, VR.UI, "1.2.3.4.5");
        deidentify(attrs);
        Assertions.assertNotNull(attrs.getString(Tag.DigitalSignatureUID), "U: must stay present");
        Assertions.assertNotEquals("1.2.3.4.5", attrs.getString(Tag.DigitalSignatureUID));
    }

    @Test
    public void prescriptionNotesSequenceIsEmptied() {
        Attributes attrs = new Attributes();
        Sequence notes = attrs.newSequence(Tag.PrescriptionNotesSequence, 1);
        Attributes item = new Attributes();
        item.setString(Tag.CodeMeaning, VR.LO, "Meier");
        notes.add(item);
        deidentify(attrs);
        Assertions.assertTrue(attrs.contains(Tag.PrescriptionNotesSequence), "Z: must stay present");
        Assertions.assertTrue(attrs.getSequence(Tag.PrescriptionNotesSequence).isEmpty());
    }

    @Test
    public void binaryAttributesWithActionDGetANonEmptyDummy() {
        Attributes attrs = new Attributes();
        attrs.setBytes(Tag.EncapsulatedDocument, VR.OB, new byte[]{37, 80, 68, 70});
        attrs.setBytes(Tag.SelectorUNValue, VR.UN, new byte[]{1, 2});
        deidentify(attrs);
        for (int tag : new int[]{Tag.EncapsulatedDocument, Tag.SelectorUNValue}) {
            byte[] value = attrs.getSafeBytes(tag);
            Assertions.assertNotNull(value);
            Assertions.assertEquals(8, value.length, "D: non-zero, even length");
            Assertions.assertArrayEquals(new byte[8], value);
        }
    }

    @Test
    public void graphicAnnotationSequenceIsRemovedBecauseOfFreeText() {
        Attributes attrs = new Attributes();
        Sequence annotations = attrs.newSequence(Tag.GraphicAnnotationSequence, 1);
        Attributes annotation = new Attributes();
        Sequence texts = annotation.newSequence(Tag.TextObjectSequence, 1);
        Attributes text = new Attributes();
        text.setString(Tag.UnformattedTextValue, VR.ST, "Meier Hans 12.3.1960");
        texts.add(text);
        annotations.add(annotation);
        deidentify(attrs);
        Assertions.assertFalse(attrs.contains(Tag.GraphicAnnotationSequence));
    }
}
