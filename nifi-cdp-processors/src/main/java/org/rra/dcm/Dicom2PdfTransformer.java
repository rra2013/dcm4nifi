package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Dicom2PdfTransformer {
    public static String transform(InputStream in, OutputStream out) throws IOException {
        return convert(in, out);
    }

    private static String convert(InputStream src, OutputStream dest) throws IOException {
        String ext;
        try (DicomInputStream dis = new DicomInputStream(src)) {
            Attributes attributes = dis.readDataset();
            String sopCUID = attributes.getString(Tag.SOPClassUID);
            ext = FileType.getFileExt(sopCUID);
            if (ext == null) {
                throw new IOException("DICOM file {} with {} SOP Class cannot be converted to bulkdata: " +
                        UID.nameOf(sopCUID));
            }
            byte[] value = (byte[]) attributes.getValue(Tag.EncapsulatedDocument);
            dest.write(value, 0, value.length - 1);
            byte lastByte = value[value.length - 1];
            if (lastByte != 0) dest.write(lastByte);
        }
        return ext;
    }

    enum FileType {
        PDF(UID.EncapsulatedPDFStorage, ".pdf"),
        CDA(UID.EncapsulatedCDAStorage, ".xml"),
        MTL(UID.EncapsulatedMTLStorage, ".mtl"),
        OBJ(UID.EncapsulatedOBJStorage, ".obj"),
        STL(UID.EncapsulatedSTLStorage, ".stl"),
        GENOZIP(UID.PrivateDcm4cheEncapsulatedGenozipStorage, ".genozip"),
        VCF_BZIP2(UID.PrivateDcm4cheEncapsulatedBzip2VCFStorage, ".vcfbz2"),
        DOC_BZIP2(UID.PrivateDcm4cheEncapsulatedBzip2DocumentStorage, ".bz2");

        private final String sopClass;
        private final String fileExt;

        FileType(String sopClass, String fileExt) {
            this.sopClass = sopClass;
            this.fileExt = fileExt;
        }

        public static String getFileExt(String sopCUID) {
            for (FileType fileType : values())
                if (fileType.getSOPClass().equals(sopCUID))
                    return fileType.getFileExt();
            return null;
        }

        private String getSOPClass() {
            return sopClass;
        }

        private String getFileExt() {
            return fileExt;
        }
    }
}
