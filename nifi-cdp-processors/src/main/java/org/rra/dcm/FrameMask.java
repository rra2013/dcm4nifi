package org.rra.dcm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Maskiert native Frames (Little Endian) direkt im Byte-Array.
 *
 * <p>Die Regionen werden einmal gegen die Bildgeometrie geprüft und in
 * Zeilenabschnitte umgerechnet; pro Frame werden nur noch diese Abschnitte
 * überschrieben. Semantik der Regionen und des Maskwerts wie
 * {@link MaskPxDataTransformer}: negative Breite = volle Bildbreite,
 * Farbwert 0xRRGGBB bzw. Komponentenwerte in Speicherreihenfolge.</p>
 */
final class FrameMask {

    private final int pixelValue;
    private final int samples;
    private final int bytesPerSample;
    private final int planarConfiguration;
    private final int planeSize;
    /** Paare (erster Pixelindex, Anzahl Pixel) je maskierter Zeile. */
    private final int[] spans;

    FrameMask(List<MaskRegion> regions, int pixelValue, int rows, int columns, int samples,
              int bytesPerSample, int planarConfiguration) {
        if (samples != 1 && samples != 3) {
            throw new IllegalArgumentException("SamplesPerPixel wird nicht unterstützt: " + samples);
        }
        if (bytesPerSample != 1 && bytesPerSample != 2) {
            throw new IllegalArgumentException("BitsAllocated wird nicht unterstützt: " + bytesPerSample * 8
                    + ". Unterstützt werden 8 und 16 Bit.");
        }
        if (samples == 3 && bytesPerSample != 1) {
            throw new IllegalArgumentException("Farbbilder werden aktuell nur mit 8 BitsAllocated unterstützt.");
        }
        this.pixelValue = pixelValue;
        this.samples = samples;
        this.bytesPerSample = bytesPerSample;
        this.planarConfiguration = planarConfiguration;
        this.planeSize = rows * columns;

        List<int[]> rowSpans = new ArrayList<>();
        for (MaskRegion r : regions) {
            int configuredX = r.getX();
            int configuredWidth = r.getWidth();
            int x = configuredWidth < 0 ? 0 : configuredX;
            int width = configuredWidth < 0 ? columns : configuredWidth;
            int y = r.getY();
            int height = r.getHeight();
            if (configuredWidth == 0 || y < 0 || height <= 0 || (configuredWidth > 0 && configuredX < 0)) {
                throw new IllegalArgumentException("Ungültige Region: x=" + configuredX + ", y=" + y
                        + ", width=" + configuredWidth + ", height=" + height
                        + ", image=" + columns + "x" + rows);
            }
            if ((long) x + width > columns || (long) y + height > rows) {
                throw new IllegalArgumentException("Region liegt ausserhalb des Bildes: x=" + configuredX
                        + ", y=" + y + ", width=" + configuredWidth + ", height=" + height
                        + ", effectiveX=" + x + ", effectiveWidth=" + width
                        + ", image=" + columns + "x" + rows);
            }
            for (int row = 0; row < height; row++) {
                rowSpans.add(new int[]{(y + row) * columns + x, width});
            }
        }
        this.spans = new int[rowSpans.size() * 2];
        for (int i = 0; i < rowSpans.size(); i++) {
            spans[2 * i] = rowSpans.get(i)[0];
            spans[2 * i + 1] = rowSpans.get(i)[1];
        }
    }

    void apply(byte[] frame) {
        for (int i = 0; i < spans.length; i += 2) {
            int first = spans[i];
            int count = spans[i + 1];
            if (samples == 1) {
                if (bytesPerSample == 1) {
                    Arrays.fill(frame, first, first + count, (byte) pixelValue);
                } else {
                    byte lo = (byte) pixelValue;
                    byte hi = (byte) (pixelValue >> 8);
                    for (int p = first * 2, end = (first + count) * 2; p < end; p += 2) {
                        frame[p] = lo;
                        frame[p + 1] = hi;
                    }
                }
            } else {
                byte c0 = (byte) (pixelValue >> 16);
                byte c1 = (byte) (pixelValue >> 8);
                byte c2 = (byte) pixelValue;
                if (planarConfiguration == 1) {
                    Arrays.fill(frame, first, first + count, c0);
                    Arrays.fill(frame, planeSize + first, planeSize + first + count, c1);
                    Arrays.fill(frame, 2 * planeSize + first, 2 * planeSize + first + count, c2);
                } else {
                    for (int p = first * 3, end = (first + count) * 3; p < end; p += 3) {
                        frame[p] = c0;
                        frame[p + 1] = c1;
                        frame[p + 2] = c2;
                    }
                }
            }
        }
    }
}
