package org.rra.processors;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Fragments;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;
import org.junit.jupiter.api.Test;
import org.rra.dcm.Dicom2DicomTranscoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Einmaliger Diagnosetest für "No implementation to handle 9 channels".
 * Aufruf mit -Ddicom.us.test.jpeg.file=/pfad/zur/datei.dcm
 */
class MaskMultiFrameUSDiagnoseTest {

    @Test
    void diagnoseJpegMultiFrame() throws Exception {
        String file = System.getProperty("dicom.us.test.jpeg.file");
        assumeTrue(file != null && Files.isRegularFile(Path.of(file)), "dicom.us.test.jpeg.file nicht gesetzt");
        byte[] bytes = Files.readAllBytes(Path.of(file));

        try (DicomInputStream dis = new DicomInputStream(new ByteArrayInputStream(bytes))) {
            Attributes fmi = dis.readFileMetaInformation();
            Attributes ds = dis.readDataset();
            System.out.println("TS=" + fmi.getString(Tag.TransferSyntaxUID)
                    + " PI=" + ds.getString(Tag.PhotometricInterpretation)
                    + " SPP=" + ds.getInt(Tag.SamplesPerPixel, -1)
                    + " PC=" + ds.getInt(Tag.PlanarConfiguration, -1)
                    + " Bits=" + ds.getInt(Tag.BitsAllocated, -1) + "/" + ds.getInt(Tag.BitsStored, -1)
                    + " Size=" + ds.getInt(Tag.Columns, -1) + "x" + ds.getInt(Tag.Rows, -1)
                    + " Frames=" + ds.getInt(Tag.NumberOfFrames, -1));

            Object value = ds.getValue(Tag.PixelData);
            if (value instanceof Fragments fragments) {
                System.out.println("Fragmente inkl. Offset-Tabelle=" + fragments.size()
                        + ", Offset-Tabelle leer=" + (((byte[]) fragments.get(0)).length == 0));
                for (int i = 1; i < Math.min(fragments.size(), 4); i++) {
                    byte[] fragment = (byte[]) fragments.get(i);
                    System.out.println("Fragment " + i + ": " + fragment.length + " Bytes, " + describeJpeg(fragment));
                }
            } else {
                System.out.println("PixelData ist nicht encapsulated: " + value);
            }
        }

        // Dreimal hintereinander dekodieren: zeigt, ob der Fehler an der
        // Datei liegt (immer) oder an Zustand im opencv-Codec (nur manchmal).
        for (int run = 1; run <= 3; run++) {
            try (InputStream in = new ByteArrayInputStream(bytes)) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Dicom2DicomTranscoder.transcode(in, out, UID.ExplicitVRLittleEndian);
                System.out.println("Decode " + run + ": OK, " + out.size() + " Bytes");
            } catch (Throwable t) {
                System.out.println("Decode " + run + ": " + t);
            }
        }
    }

    private static String describeJpeg(byte[] b) {
        StringBuilder s = new StringBuilder();
        int soi = 0;
        for (int i = 0; i + 1 < b.length; i++) {
            if ((b[i] & 0xFF) != 0xFF) {
                continue;
            }
            int marker = b[i + 1] & 0xFF;
            if (marker == 0xD8) {
                soi++;
            } else if (marker == 0xE0 && i + 8 < b.length && b[i + 4] == 'J' && b[i + 5] == 'F') {
                s.append("JFIF ");
            } else if (marker == 0xEE && i + 15 < b.length && b[i + 4] == 'A' && b[i + 5] == 'd') {
                s.append("Adobe(transform=").append(b[i + 15] & 0xFF).append(") ");
            } else if (soi == 1 && marker >= 0xC0 && marker <= 0xCF
                    && marker != 0xC4 && marker != 0xC8 && marker != 0xCC && i + 9 < b.length) {
                int components = b[i + 9] & 0xFF;
                s.append(String.format("SOF%02X comps=%d [", marker, components));
                for (int c = 0; c < components && i + 12 + 3 * c < b.length; c++) {
                    int id = b[i + 10 + 3 * c] & 0xFF;
                    int sampling = b[i + 11 + 3 * c] & 0xFF;
                    s.append(String.format(" id=%d %dx%d", id, sampling >> 4, sampling & 0xF));
                }
                s.append(" ] ");
            }
        }
        return s.append("SOI-Anzahl=").append(soi).toString();
    }
}
