package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.image.ICCProfile;
import org.dcm4che3.imageio.plugins.dcm.DicomImageReadParam;
import org.dcm4che3.io.DicomInputStream;

import javax.imageio.*;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.*;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class Dicom2JpegTransformer {
    private final ImageReader imageReader =
            ImageIO.getImageReadersByFormatName("DICOM").next();
    private ReadImage readImage;
    private int windowIndex;
    private int voiLUTIndex;
    private boolean preferWindow = true;
    private float windowCenter;
    private float windowWidth;
    private boolean autoWindowing = true;
    private boolean ignorePresentationLUTShape;
    private Attributes prState;
    private ImageWriter imageWriter;
    private ImageWriteParam imageWriteParam;
    private final int overlayActivationMask = 0xffff;
    private final int overlayGrayscaleValue = 0xffff;
    private final int overlayRGBValue = 0xffffff;
    private final ICCProfile.Option iccProfile = ICCProfile.Option.none;

    public Dicom2JpegTransformer() {
        init();
    }

    private static Predicate<Object> matchClassName(String clazz) {
        Predicate<String> predicate = clazz.endsWith("*")
                ? startsWith(clazz.substring(0, clazz.length() - 1))
                : clazz::equals;
        return w -> predicate.test(w.getClass().getName());
    }

    private static Predicate<String> startsWith(String prefix) {
        return s -> s.startsWith(prefix);
    }

    private void init() {
        // Init the writer
        initImageWriter("JPEG", "com.sun.imageio.plugins.*", null, .85);
        //initImageWriter("PNG",this.suffix,"com.sun.imageio.plugins.*",null, null);
        this.windowIndex = 0;
        this.voiLUTIndex = 0;
        this.preferWindow = true;
        this.autoWindowing = true;
        this.ignorePresentationLUTShape = false;
        this.readImage = this::readImageFromDicomInputStream;
    }

    private BufferedImage readImageFromDicomInputStream(int frame, InputStream inputStream) throws IOException {
        try (DicomInputStream dis = new DicomInputStream(inputStream)) {
            imageReader.setInput(dis);
            return imageReader.read(frame - 1, readParam());
        }
    }

    private ImageReadParam readParam() {
        DicomImageReadParam param =
                (DicomImageReadParam) imageReader.getDefaultReadParam();
        param.setWindowCenter(windowCenter);
        param.setWindowWidth(windowWidth);
        param.setAutoWindowing(autoWindowing);
        param.setIgnorePresentationLUTShape(ignorePresentationLUTShape);
        param.setWindowIndex(windowIndex);
        param.setVOILUTIndex(voiLUTIndex);
        param.setPreferWindow(preferWindow);
        param.setPresentationState(prState);
        param.setOverlayActivationMask(overlayActivationMask);
        param.setOverlayGrayscaleValue(overlayGrayscaleValue);
        param.setOverlayRGBValue(overlayRGBValue);
        return param;
    }

    private void initImageWriter(String formatName, String clazz, String compressionType, Number quality) {
        Iterator<ImageWriter> imageWriters = ImageIO.getImageWritersByFormatName(formatName);
        if (!imageWriters.hasNext())
            throw new IllegalArgumentException(
                    MessageFormat.format("FormatNotSupported: {0}", formatName)
            );
        Iterable<ImageWriter> iterable = () -> imageWriters;
        imageWriter = StreamSupport.stream(iterable.spliterator(), false)
                .filter(matchClassName(clazz))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        MessageFormat.format("NoSuchImageWriter: {0} {1}", clazz, formatName)));
        imageWriteParam = imageWriter.getDefaultWriteParam();
        if (compressionType != null || quality != null) {
            imageWriteParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            if (compressionType != null)
                imageWriteParam.setCompressionType(compressionType);
            if (quality != null)
                imageWriteParam.setCompressionQuality(quality.floatValue());
        }
    }


    public void transform(int frame, InputStream src, ImageOutputStream dest) throws IOException {
        BufferedImage rmg = readImage.apply(frame, src);
        rmg = iccProfile.adjust(rmg);
        writeImage(dest, rmg);
    }

    private void writeImage(ImageOutputStream dest, BufferedImage bi) throws IOException {
        imageWriter.setOutput(dest);
        imageWriter.write(null, new IIOImage(bi, null, null), imageWriteParam);
    }

    private interface ReadImage {
        BufferedImage apply(int frame, InputStream src) throws IOException;
    }
}