package org.rra.dcm;

import org.dcm4che3.data.UID;

import java.util.Optional;
import java.util.stream.Stream;

public enum DcmObjectType {
    UncompressedSingleFrameImage(MediaTypes.IMAGE_JPEG_TYPE, true, false) {
        @Override
        public Optional<MediaType> getCompatibleMimeType(MediaType other) {
            return findCompatibleSingleFrameMimeType(other);
        }

        @Override
        public MediaType[] getRenderedContentTypes() {
            return DcmObjectType.renderedSingleFrameMediaTypes();
        }

        @Override
        public MediaType[] getBulkdataContentTypes(SOPClassInfo inst) {
            return octetStreamMediaType();
        }
    },
    CompressedSingleFrameImage(MediaTypes.IMAGE_JPEG_TYPE, true, false) {
        @Override
        public Optional<MediaType> getCompatibleMimeType(MediaType other) {
            return findCompatibleSingleFrameMimeType(other);
        }

        @Override
        public MediaType[] getRenderedContentTypes() {
            return renderedSingleFrameMediaTypes();
        }

        @Override
        public MediaType[] getBulkdataContentTypes(SOPClassInfo inst) {
            return calcPixelDataContentTypes(inst);
        }
    },
    UncompressedMultiFrameImage(MediaTypes.APPLICATION_DICOM_TYPE, true, false) {
        @Override
        public Optional<MediaType> getCompatibleMimeType(MediaType other) {
            return findCompatibleMultiFrameMimeType(other);
        }

        @Override
        public MediaType[] getRenderedContentTypes() {
            return renderedMultiFrameMediaTypes();
        }

        @Override
        public MediaType[] getBulkdataContentTypes(SOPClassInfo inst) {
            return octetStreamMediaType();
        }
    },
    CompressedMultiFrameImage(MediaTypes.APPLICATION_DICOM_TYPE, true, false) {
        @Override
        public Optional<MediaType> getCompatibleMimeType(MediaType other) {
            return findCompatibleMultiFrameMimeType(other);
        }

        @Override
        public MediaType[] getRenderedContentTypes() {
            return renderedMultiFrameMediaTypes();
        }

        @Override
        public MediaType[] getBulkdataContentTypes(SOPClassInfo inst) {
            return calcPixelDataContentTypes(inst);
        }
    },
    MPEG2Video(MediaTypes.VIDEO_MPEG_TYPE, false, true),
    MPEG4Video(MediaTypes.VIDEO_MP4_TYPE, false, true),
    SRDocument(MediaType.TEXT_HTML_TYPE, false, false) {
        @Override
        public Optional<MediaType> getCompatibleMimeType(MediaType other) {
            return findCompatibleSRMimeType(other);
        }

        @Override
        public MediaType[] getRenderedContentTypes() {
            return renderedSRMediaTypes();
        }

        @Override
        public MediaType[] getBulkdataContentTypes(SOPClassInfo inst) {
            return null;
        }
    },
    EncapsulatedPDF(MediaTypes.APPLICATION_PDF_TYPE, false, false),
    EncapsulatedCDA(MediaType.TEXT_XML_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }
    },
    EncapsulatedSTL(MediaTypes.MODEL_STL_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }
    },
    EncapsulatedOBJ(MediaTypes.MODEL_OBJ_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }
    },
    EncapsulatedMTL(MediaTypes.MODEL_MTL_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }
    },
    EncapsulatedGenozip(MediaTypes.APPLICATION_VND_GENOZIP_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }
    },
    EncapsulatedVCFBzip2(MediaTypes.APPLICATION_PRS_VCFBZIP2_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }
    },
    EncapsulatedBzip2(MediaTypes.APPLICATION_X_BZIP2_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }
    },
    Other(MediaTypes.APPLICATION_DICOM_TYPE, false, false){
        @Override
        public MediaType[] getRenderedContentTypes() {
            return null;
        }

        @Override
        public MediaType[] getBulkdataContentTypes(SOPClassInfo inst) {
            return null;
        }
    };

    private final MediaType defaultMimeType;
    private final boolean image;
    private final boolean video;

    DcmObjectType(MediaType defaultMimeType, boolean image, boolean video) {
        this.defaultMimeType = defaultMimeType;
        this.image = image;
        this.video = video;
    }

    public static DcmObjectType objectTypeOf(SOPClassInfo inst) {
        if (inst.isImage()) {
            switch (inst.getTransferSyntaxUID()) {
                case UID.MPEG2MPML:
                case UID.MPEG2MPMLF:
                case UID.MPEG2MPHL:
                case UID.MPEG2MPHLF:
                    return MPEG2Video;
                case UID.MPEG4HP41:
                case UID.MPEG4HP41F:
                case UID.MPEG4HP41BD:
                case UID.MPEG4HP41BDF:
                case UID.MPEG4HP422D:
                case UID.MPEG4HP422DF:
                case UID.MPEG4HP423D:
                case UID.MPEG4HP423DF:
                case UID.MPEG4HP42STEREO:
                case UID.MPEG4HP42STEREOF:
                case UID.HEVCMP51:
                case UID.HEVCM10P51:
                    return MPEG4Video;
                case UID.ImplicitVRLittleEndian:
                case UID.ExplicitVRLittleEndian:
                    return inst.isMultiframe()
                            ? UncompressedMultiFrameImage
                            : UncompressedSingleFrameImage;
                default:
                    return inst.isMultiframe()
                            ? CompressedMultiFrameImage
                            : CompressedSingleFrameImage;
            }
        }
        switch (inst.getSopClassUID()) {
            case UID.EncapsulatedPDFStorage:
                return EncapsulatedPDF;
            case UID.EncapsulatedCDAStorage:
                return EncapsulatedCDA;
            case UID.EncapsulatedSTLStorage:
                return EncapsulatedSTL;
            case UID.EncapsulatedMTLStorage:
                return EncapsulatedMTL;
            case UID.EncapsulatedOBJStorage:
                return EncapsulatedOBJ;
            case UID.PrivateDcm4cheEncapsulatedGenozipStorage:
                return EncapsulatedGenozip;
            case UID.PrivateDcm4cheEncapsulatedBzip2VCFStorage:
                return EncapsulatedVCFBzip2;
            case UID.PrivateDcm4cheEncapsulatedBzip2DocumentStorage:
                return EncapsulatedBzip2;
        }
        return (inst.isReport())
                ? SRDocument
                : Other;
    }

    public MediaType getDefaultMimeType() {
        return defaultMimeType;
    }

    public Optional<MediaType> getCompatibleMimeType(MediaType other) {
        return findCompatibleMimeType(other, defaultMimeType, MediaTypes.APPLICATION_DICOM_TYPE);
    }

    private static MediaType[] octetStreamMediaType() {
        return new MediaType[]{MediaType.APPLICATION_OCTET_STREAM_TYPE};
    }

    private static MediaType[] renderedSingleFrameMediaTypes() {
        return new MediaType[]{MediaTypes.IMAGE_JPEG_TYPE, MediaTypes.IMAGE_GIF_TYPE, MediaTypes.IMAGE_PNG_TYPE};
    }

    private static MediaType[] renderedMultiFrameMediaTypes() {
        return new MediaType[]{MediaTypes.IMAGE_GIF_TYPE};
    }

    private static MediaType[] renderedSRMediaTypes() {
        return new MediaType[]{MediaType.TEXT_HTML_TYPE, MediaType.TEXT_PLAIN_TYPE};
    }

    private static Optional<MediaType> findCompatibleSingleFrameMimeType(MediaType other) {
        return findCompatibleMimeType(other,
                MediaTypes.IMAGE_JPEG_TYPE,
                MediaTypes.APPLICATION_DICOM_TYPE,
                MediaTypes.IMAGE_GIF_TYPE,
                MediaTypes.IMAGE_PNG_TYPE);
    }

    private static Optional<MediaType> findCompatibleMultiFrameMimeType(MediaType other) {
        return findCompatibleMimeType(other,
                MediaTypes.APPLICATION_DICOM_TYPE,
                MediaTypes.IMAGE_GIF_TYPE);
    }

    private static Optional<MediaType> findCompatibleSRMimeType(MediaType other) {
        return findCompatibleMimeType(other,
                MediaType.TEXT_HTML_TYPE,
                MediaType.TEXT_PLAIN_TYPE,
                MediaTypes.APPLICATION_DICOM_TYPE);
    }

    private static Optional<MediaType> findCompatibleMimeType(MediaType other, MediaType... mimeTypes) {
        return Stream.of(mimeTypes).filter(other::isCompatible).findFirst();
    }

    public MediaType[] getRenderedContentTypes() {
        return new MediaType[]{defaultMimeType};
    }

    public MediaType[] getBulkdataContentTypes(SOPClassInfo inst) {

        return new MediaType[]{defaultMimeType};
    }

    public boolean isImage() {
        return image;
    }

    public boolean isVideo() {
        return video;
    }

    private static MediaType[] calcPixelDataContentTypes(SOPClassInfo inst) {
        String tsuid = inst.getTransferSyntaxUID();
        MediaType mediaType = MediaTypes.forTransferSyntax(tsuid);
        return new MediaType[] {mediaType, MediaType.APPLICATION_OCTET_STREAM_TYPE};
    }
}
