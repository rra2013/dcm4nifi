package org.rra.dcm;

import lombok.Getter;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;

import java.util.Arrays;
import java.util.List;

@Getter
public class SOPClassInfo {
    private final Attributes attributes;
    private final String transferSyntaxUID;
    private final String sopClassUID;


    public SOPClassInfo(Attributes attrs, String transferSyntaxUID) {
        this.attributes = attrs;
        this.transferSyntaxUID = transferSyntaxUID;
        this.sopClassUID = attrs.getString(Tag.SOPClassUID);
    }

    public boolean isImage() {
        return attributes.contains(Tag.BitsAllocated) && !sopClassUID.equals(UID.RTDoseStorage);
    }

    public boolean isVideo() {
        switch (this.transferSyntaxUID) {
            case UID.MPEG2MPML:
            case UID.MPEG2MPMLF:
            case UID.MPEG2MPHL:
            case UID.MPEG2MPHLF:
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
                return true;
        }
        return false;
    }

    public boolean isMultiframe() {
        return attributes.getInt(Tag.NumberOfFrames, 1) > 1;
    }

    public boolean isReport(){
        List<String> srTs = Arrays.asList(SR_TSUIDS);
        boolean tsOK = srTs.contains(this.transferSyntaxUID);
        List<String> list = Arrays.asList(SR_CUIDS);
        return list.contains(this.sopClassUID) && tsOK;
    }

    private static final String[] SR_CUIDS = {
            UID.SpectaclePrescriptionReportStorage,
            UID.MacularGridThicknessAndVolumeReportStorage,
            UID.BasicTextSRStorage,
            UID.EnhancedSRStorage,
            UID.ComprehensiveSRStorage,
            UID.Comprehensive3DSRStorage,
            UID.ExtensibleSRStorage,
            UID.ProcedureLogStorage,
            UID.MammographyCADSRStorage,
            UID.KeyObjectSelectionDocumentStorage,
            UID.ChestCADSRStorage,
            UID.XRayRadiationDoseSRStorage,
            UID.RadiopharmaceuticalRadiationDoseSRStorage,
            UID.ColonCADSRStorage,
            UID.ImplantationPlanSRStorage,
            UID.AcquisitionContextSRStorage,
            UID.SimplifiedAdultEchoSRStorage,
            UID.PatientRadiationDoseSRStorage,
            UID.PlannedImagingAgentAdministrationSRStorage,
            UID.PerformedImagingAgentAdministrationSRStorage,
            UID.EnhancedXRayRadiationDoseSRStorage,
            UID.WaveformAnnotationSRStorage
    };
    static final String[] OTHER_CUIDS = {
            UID.StoredPrintStorage,
            UID.HardcopyGrayscaleImageStorage,
            UID.HardcopyColorImageStorage,
            UID.MRSpectroscopyStorage,
            UID.MultiFrameSingleBitSecondaryCaptureImageStorage,
            UID.StandaloneOverlayStorage,
            UID.StandaloneCurveStorage,
            UID.TwelveLeadECGWaveformStorage,
            UID.GeneralECGWaveformStorage,
            UID.General32bitECGWaveformStorage,
            UID.AmbulatoryECGWaveformStorage,
            UID.HemodynamicWaveformStorage,
            UID.CardiacElectrophysiologyWaveformStorage,
            UID.BasicVoiceAudioWaveformStorage,
            UID.GeneralAudioWaveformStorage,
            UID.ArterialPulseWaveformStorage,
            UID.RespiratoryWaveformStorage,
            UID.MultichannelRespiratoryWaveformStorage,
            UID.RoutineScalpElectroencephalogramWaveformStorage,
            UID.ElectromyogramWaveformStorage,
            UID.ElectrooculogramWaveformStorage,
            UID.SleepElectroencephalogramWaveformStorage,
            UID.BodyPositionWaveformStorage,
            UID.StandaloneModalityLUTStorage,
            UID.StandaloneVOILUTStorage,
            UID.GrayscaleSoftcopyPresentationStateStorage,
            UID.ColorSoftcopyPresentationStateStorage,
            UID.PseudoColorSoftcopyPresentationStateStorage,
            UID.BlendingSoftcopyPresentationStateStorage,
            UID.XAXRFGrayscaleSoftcopyPresentationStateStorage,
            UID.GrayscalePlanarMPRVolumetricPresentationStateStorage,
            UID.CompositingPlanarMPRVolumetricPresentationStateStorage,
            UID.AdvancedBlendingPresentationStateStorage,
            UID.VolumeRenderingVolumetricPresentationStateStorage,
            UID.SegmentedVolumeRenderingVolumetricPresentationStateStorage,
            UID.MultipleVolumeRenderingVolumetricPresentationStateStorage,
            UID.VariableModalityLUTSoftcopyPresentationStateStorage,
            UID.ParametricMapStorage,
            UID.RawDataStorage,
            UID.SpatialRegistrationStorage,
            UID.SpatialFiducialsStorage,
            UID.DeformableSpatialRegistrationStorage,
            UID.SegmentationStorage,
            UID.SurfaceSegmentationStorage,
            UID.TractographyResultsStorage,
            UID.LabelMapSegmentationStorage,
            UID.HeightMapSegmentationStorage,
            UID.RealWorldValueMappingStorage,
            UID.SurfaceScanMeshStorage,
            UID.SurfaceScanPointCloudStorage,
            UID.StereometricRelationshipStorage,
            UID.LensometryMeasurementsStorage,
            UID.AutorefractionMeasurementsStorage,
            UID.KeratometryMeasurementsStorage,
            UID.SubjectiveRefractionMeasurementsStorage,
            UID.VisualAcuityMeasurementsStorage,
            UID.OphthalmicAxialMeasurementsStorage,
            UID.IntraocularLensCalculationsStorage,
            UID.OphthalmicVisualFieldStaticPerimetryMeasurementsStorage,
            UID.BasicStructuredDisplayStorage,
            UID.EncapsulatedPDFStorage,
            UID.EncapsulatedCDAStorage,
            UID.EncapsulatedSTLStorage,
            UID.EncapsulatedOBJStorage,
            UID.EncapsulatedMTLStorage,
            UID.StandalonePETCurveStorage,
            UID.TextSRStorageTrial,
            UID.AudioSRStorageTrial,
            UID.DetailSRStorageTrial,
            UID.ComprehensiveSRStorageTrial,
            UID.ContentAssessmentResultsStorage,
            UID.MicroscopyBulkSimpleAnnotationsStorage,
            UID.CTPerformedProcedureProtocolStorage,
            UID.XAPerformedProcedureProtocolStorage,
            UID.RTDoseStorage,
            UID.RTStructureSetStorage,
            UID.RTBeamsTreatmentRecordStorage,
            UID.RTPlanStorage,
            UID.RTBrachyTreatmentRecordStorage,
            UID.RTTreatmentSummaryRecordStorage,
            UID.RTIonPlanStorage,
            UID.RTIonBeamsTreatmentRecordStorage,
            UID.RTPhysicianIntentStorage,
            UID.RTSegmentAnnotationStorage,
            UID.RTRadiationSetStorage,
            UID.CArmPhotonElectronRadiationStorage,
            UID.TomotherapeuticRadiationStorage,
            UID.RoboticArmRadiationStorage,
            UID.RTRadiationRecordSetStorage,
            UID.RTRadiationSalvageRecordStorage,
            UID.TomotherapeuticRadiationRecordStorage,
            UID.CArmPhotonElectronRadiationRecordStorage,
            UID.RoboticRadiationRecordStorage,
            UID.RTRadiationSetDeliveryInstructionStorage,
            UID.RTTreatmentPreparationStorage,
            UID.RTPatientPositionAcquisitionInstructionStorage,
            UID.RTBeamsDeliveryInstructionStorage,
            UID.RTBrachyApplicationSetupDeliveryInstructionStorage,
    };

    static final String[] PRIVATE_CUIDS = {
            UID.PrivateDcm4cheEncapsulatedGenozipStorage,
            UID.PrivateDcm4cheEncapsulatedBzip2VCFStorage,
            UID.PrivateDcm4cheEncapsulatedBzip2DocumentStorage,
            UID.PrivateAgfaArrivalTransaction,
            UID.PrivateAgfaBasicAttributePresentationState,
            UID.PrivateAgfaDictationTransaction,
            UID.PrivateAgfaReportApprovalTransaction,
            UID.PrivateAgfaReportTranscriptionTransaction,
            UID.PrivateERADPracticeBuilderReportDictationStorage,
            UID.PrivateERADPracticeBuilderReportTextStorage,
            UID.PrivateGE3DModelStorage,
            UID.PrivateGECollageStorage,
            UID.PrivateGEeNTEGRAProtocolOrNMGenieStorage,
            UID.PrivateGEPETRawDataStorage,
            UID.PrivateGERTPlanStorage,
            UID.PrivatePhilips3DObjectStorage,
            UID.PrivatePhilips3DObjectStorageRetired,
            UID.PrivatePhilips3DPresentationStateStorage,
            UID.PrivatePhilipsCompositeObjectStorage,
            UID.PrivatePhilipsHPLive3D01Storage,
            UID.PrivatePhilipsHPLive3D02Storage,
            UID.PrivatePhilipsLiveRunStorage,
            UID.PrivatePhilipsMRCardioAnalysisStorage,
            UID.PrivatePhilipsMRCardioAnalysisStorageRetired,
            UID.PrivatePhilipsMRCardioProfileStorage,
            UID.PrivatePhilipsMRCardioStorage,
            UID.PrivatePhilipsMRCardioStorageRetired,
            UID.PrivatePhilipsMRExamcardStorage,
            UID.PrivatePhilipsMRSeriesDataStorage,
            UID.PrivatePhilipsMRSpectrumStorage,
            UID.PrivatePhilipsPerfusionStorage,
            UID.PrivatePhilipsReconstructionStorage,
            UID.PrivatePhilipsRunStorage,
            UID.PrivatePhilipsSpecialisedXAStorage,
            UID.PrivatePhilipsSurfaceStorage,
            UID.PrivatePhilipsSurfaceStorageRetired,
            UID.PrivatePhilipsVolumeSetStorage,
            UID.PrivatePhilipsVolumeStorage,
            UID.PrivatePhilipsVolumeStorageRetired,
            UID.PrivatePhilipsVRMLStorage,
            UID.PrivatePhilipsXRayMFStorage,
            UID.PrivateSiemensAXFrameSetsStorage,
            UID.PrivateSiemensCSANonImageStorage,
            UID.PrivateSiemensCTMRVolumeStorage,
            UID.PrivateTomTecAnnotationStorage
    };
    static final String[] SR_TSUIDS = {
            UID.ImplicitVRLittleEndian,
            UID.ExplicitVRLittleEndian,
            UID.DeflatedExplicitVRLittleEndian
    };

}
