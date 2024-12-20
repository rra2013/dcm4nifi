package org.rra.dcm;

import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.imageio.codec.Transcoder;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.util.Property;

import java.io.*;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Dcm2DcmTranscoder {
    private static DicomEncodingOptions encOpts = DicomEncodingOptions.DEFAULT;
    private static final List<Property> params = new ArrayList<>();

    public static void transcode(InputStream inputStream, OutputStream outputStream, String ts) throws IOException {
        transcodeWithTranscoder(inputStream, outputStream, ts);
    }

    private static void transcodeWithTranscoder( final InputStream inputStream, final OutputStream outputStream, String tsuid) throws IOException {
        try (Transcoder transcoder = new Transcoder(inputStream)) {
            transcoder.setIncludeFileMetaInformation(false);
            transcoder.setRetainFileMetaInformation(false);
            transcoder.setEncodingOptions(encOpts);
            transcoder.setDestinationTransferSyntax(tsuid);
            transcoder.setCompressParams(params.toArray(new Property[params.size()]));
            transcoder.transcode((transcoder1, dataset) -> outputStream);
        } catch (Exception e) {
            throw e;
        }
    }
}
