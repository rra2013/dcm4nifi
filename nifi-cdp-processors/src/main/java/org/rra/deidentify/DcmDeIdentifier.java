package org.rra.deidentify;

import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.*;
import org.dcm4che3.dcmr.DeIdentificationMethod;
import org.dcm4che3.util.UIDUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;


@Slf4j
public class DcmDeIdentifier {
    private static final int[] X = {
            Tag.AcquisitionComments,
            Tag.AcquisitionContextSequence,
            Tag.AcquisitionProtocolDescription,
            Tag.ActualHumanPerformersSequence,
            Tag.AdditionalPatientHistory,
            Tag.AddressTrial,
            Tag.AdmissionID,
            Tag.AdmittingDiagnosesCodeSequence,
            Tag.AdmittingDiagnosesDescription,
            Tag.Allergies,
            Tag.Arbitrary,
            Tag.AuthorObserverSequence,
            Tag.BranchOfService,
            Tag.CommentsOnThePerformedProcedureStep,
            Tag.ConfidentialityConstraintOnPatientDataDescription,
            Tag.ConsultingPhysicianIdentificationSequence,
            Tag.ContentCreatorIdentificationCodeSequence,
            Tag.ContentSequence,
            Tag.ContributionDescription,
            Tag.CountryOfResidence,
            Tag.CurrentObserverTrial,
            Tag.CurrentPatientLocation,
            Tag.CustodialOrganizationSequence,
//            Tag.Date, // Content Item Attribute
            Tag.DateTime, // Content Item Attribute
            Tag.DataSetTrailingPadding,
            Tag.DerivationDescription,
            Tag.DigitalSignatureUID,
            Tag.DigitalSignaturesSequence,
            Tag.DischargeDiagnosisCodeSequence,
            Tag.DischargeDiagnosisDescription,
            Tag.DistributionAddress,
            Tag.DistributionName,
            Tag.EthnicGroup,
            Tag.FrameComments,
            Tag.GraphicAnnotationSequence,
            Tag.GPSAltitude,
            Tag.HumanPerformerCodeSequence, // missing in Part 15
            Tag.HumanPerformerName,
            Tag.HumanPerformerOrganization,
            Tag.IconImageSequence,
            Tag.IdentifyingComments,
            Tag.ImageComments,
            Tag.ImagePresentationComments,
            Tag.ImagingServiceRequestComments,
            Tag.Impressions,
            Tag.InsurancePlanIdentification,
            Tag.IntendedRecipientsOfResultsIdentificationSequence,
            Tag.InterpretationApproverSequence,
            Tag.InterpretationAuthor,
            Tag.InterpretationDiagnosisDescription,
            Tag.InterpretationIDIssuer,
            Tag.InterpretationRecorder,
            Tag.InterpretationText,
            Tag.InterpretationTranscriber,
            Tag.IssuerOfAccessionNumberSequence, // missing in Part 15
            Tag.IssuerOfAdmissionID,
            Tag.IssuerOfAdmissionIDSequence, // missing in Part 15
            Tag.IssuerOfPatientID,
            Tag.IssuerOfPatientIDQualifiersSequence, // missing in Part 15
            Tag.IssuerOfServiceEpisodeID,
            Tag.MAC,
            Tag.MedicalAlerts,
            Tag.MedicalRecordLocator,
            Tag.MilitaryRank,
            Tag.ModifiedAttributesSequence,
            Tag.ModifiedImageDescription,
            Tag.ModifyingDeviceID,
            Tag.NameOfPhysiciansReadingStudy,
            Tag.NamesOfIntendedRecipientsOfResults,
            Tag.Occupation,
            Tag.OperatorIdentificationSequence,
            Tag.OrderCallbackPhoneNumber,
            Tag.OrderCallbackTelecomInformation,
            Tag.OrderEnteredBy,
            Tag.OrderEntererLocation,
            Tag.OriginalAttributesSequence,
            Tag.OtherPatientIDs,
            Tag.OtherPatientIDsSequence,
            Tag.OtherPatientNames,
            Tag.ParticipantSequence,
            Tag.PatientAddress,
            Tag.PatientComments,
            Tag.PatientState,
            Tag.PatientTransportArrangements,
            Tag.PatientAge,
            Tag.PatientBirthName,
            Tag.PatientBirthTime,
            Tag.PatientInstitutionResidence,
            Tag.PatientInsurancePlanCodeSequence,
            Tag.PatientMotherBirthName,
            Tag.PatientPrimaryLanguageCodeSequence,
            Tag.PatientPrimaryLanguageModifierCodeSequence,
            Tag.PatientReligiousPreference,
            Tag.PatientSize,
            Tag.PatientSizeCodeSequence, // missing in Part 15
            Tag.PatientTelecomInformation,
            Tag.PatientTelephoneNumbers,
            Tag.PatientWeight,
            Tag.PerformedLocation,
            Tag.PerformedProcedureStepDescription,
            Tag.PerformedProcedureStepID,
            Tag.PerformingPhysicianIdentificationSequence,
            Tag.PerformingPhysicianName,
            Tag.PersonAddress,
            Tag.PersonIdentificationCodeSequence,
//            Tag.PersonName, // Content Item Attribute
            Tag.PersonTelecomInformation,
            Tag.PersonTelephoneNumbers,
            Tag.PhysicianApprovingInterpretation,
            Tag.PhysiciansReadingStudyIdentificationSequence,
            Tag.PhysiciansOfRecord,
            Tag.PhysiciansOfRecordIdentificationSequence,
            Tag.PreMedication,
            Tag.PregnancyStatus,
            Tag.ReasonForOmissionDescription,
            Tag.ReasonForTheImagingServiceRequest,
            Tag.ReasonForStudy,
            Tag.ReferencedDigitalSignatureSequence,
            Tag.ReferencedPatientAliasSequence,
            Tag.ReferencedPatientPhotoSequence,
            Tag.ReferencedPatientSequence,
            Tag.ReferencedSOPInstanceMACSequence,
            Tag.ReferringPhysicianAddress,
            Tag.ReferringPhysicianIdentificationSequence,
            Tag.ReferringPhysicianTelephoneNumbers,
            Tag.RegionOfResidence,
            Tag.RequestAttributesSequence,
            Tag.RequestedContrastAgent,
            Tag.RequestedProcedureComments,
            Tag.RequestedProcedureID,
            Tag.RequestedProcedureLocation,
            Tag.RequestingPhysician,
            Tag.RequestingPhysicianIdentificationSequence, // missing in Part 15
            Tag.RequestingService,
            Tag.RequestingServiceCodeSequence, // missing in Part 15
            Tag.ResponsibleOrganization,
            Tag.ResponsiblePerson,
            Tag.ResultsComments,
            Tag.ResultsDistributionListSequence,
            Tag.ResultsIDIssuer,
            Tag.ScheduledHumanPerformersSequence,
            Tag.ScheduledPatientInstitutionResidence,
            Tag.ScheduledPerformingPhysicianIdentificationSequence,
            Tag.ScheduledPerformingPhysicianName,
            Tag.ScheduledProcedureStepDescription,
            Tag.SeriesDescription,
            Tag.SeriesDescriptionCodeSequence, // missing in Part 15
            Tag.ServiceEpisodeDescription,
            Tag.ServiceEpisodeID,
            Tag.SmokingStatus,
            Tag.SpecialNeeds,
            Tag.StudyComments,
            Tag.StudyDescription,
            Tag.StudyIDIssuer,
            Tag.TelephoneNumberTrial,
            Tag.TextComments,
            Tag.TextString,
            Tag.TextValue, // Content Item Attribute
//            Tag.Time, // Content Item Attribute
            Tag.TopicAuthor,
            Tag.TopicKeywords,
            Tag.TopicSubject,
            Tag.TopicTitle,
            Tag.VerbalSourceTrial,
            Tag.VerbalSourceIdentifierCodeSequenceTrial,
            Tag.VisitComments,
            //InselGruppe
            Tag.SourceApplicationEntityTitle,
            Tag.RTReferencedStudySequence, // CAIRO
            Tag.ReferencedSeriesSequence,
            Tag.RTReferencedSeriesSequence, //CAIRO
            Tag.PerformedSeriesSequence,
            Tag.PerformedProcedureStepStatus,
            Tag.WaveformFilterDescription,
            Tag.XRayDetectorLabel,
            Tag.UniqueDeviceIdentifier,
            Tag.UDISequence,
            Tag.TreatmentSites,
            Tag.TreatmentMachineName,
            Tag.TransducerIdentificationSequence,
            Tag.TimeOfDocumentCreationOrVerbalTransactionTrial,
            Tag.TimeOfLastCalibration,
            Tag.TimeOfLastDetectorCalibration,
            Tag.TimeOfSecondaryCapture,
            Tag.TemplateLocalVersion,
            Tag.TemplateVersion,
            Tag.StructureSetName,
            Tag.StructureSetDescription,
            Tag.SpecimenShortDescription,
            Tag.SpecimenAccessionNumber,
            Tag.SpecimenDetailedDescription,
            Tag.SourceManufacturer,
            Tag.SourceImageSequence,
            Tag.SOPAuthorizationDateTime,
            Tag.SetupTechniqueDescription,
            Tag.ShieldingDeviceDescription,
            Tag.SlideIdentifier,
            Tag.StationAETitle,
            Tag.ScheduledProcedureStepID,
            Tag.RTTreatmentApproachLabel,
            Tag.RTPlanName,
            Tag.RTPlanDescription,
            Tag.ROIObservationDescription,
            Tag.ROIObservationLabel,
            Tag.ROIDescription,
            Tag.ROIGenerationDescription,
            Tag.RetrieveAETitle,
            Tag.ResultsID,
            Tag.RespiratoryMotionCompensationTechniqueDescription,
            Tag.RequestingAE,
            Tag.RequestedSeriesDescription,
            Tag.RequestedContrastAgent,
            Tag.ReferencedImageSequence,
            Tag.ReasonForTheRequestedProcedure,
            Tag.ReasonForVisit,
            Tag.ReasonForVisitCodeSequence,
            Tag.ReceivingAE,
            Tag.ReasonForRequestedProcedureCodeSequence,
            Tag.ProtocolName,
            Tag.PyramidDescription,
            Tag.PyramidLabel,
            Tag.PriorTreatmentDoseDescription,
            Tag.PrescriptionDescription,
            Tag.PositionAcquisitionTemplateDescription,
            Tag.PositionAcquisitionTemplateName,
            Tag.PatientTreatmentPreparationMethodDescription,
            Tag.PatientTreatmentPreparationProcedureParameterDescription,
            Tag.PatientSetupPhotoDescription,
            Tag.PatientWeight,
            Tag.OverlayComments,
            Tag.OverlayData,
            Tag.Originator,
            Tag.NetworkID,
            Tag.NonconformingDataElementValue,
            Tag.NonconformingModifiedAttributesSequence,
            Tag.MostRecentTreatmentDate,
            Tag.MultienergyAcquisitionDescription,
            Tag.MakerNote,
            Tag.LensMake,
            Tag.LensModel,
            Tag.LensSerialNumber,
            Tag.LensSpecification,
            Tag.LongDeviceDescription,
            Tag.IssueTimeOfImagingServiceRequest,
            Tag.LabelText,
            Tag.IssuerOfServiceEpisodeIDSequence,
            Tag.IssuerOfAdmissionIDSequence,
            Tag.IssueDateOfImagingServiceRequest,
            Tag.InterpretationID,
            Tag.InstitutionalDepartmentTypeCodeSequence,
            Tag.InstanceOriginStatus,
            Tag.GPSAltitudeRef,
            Tag.GPSAreaInformation,
            Tag.GPSDateStamp,
            Tag.GPSDestBearing,
            Tag.GPSDestBearingRef,
            Tag.GPSDestDistance,
            Tag.GPSDestDistanceRef,
            Tag.GPSDestLatitude,
            Tag.GPSDestLatitudeRef,
            Tag.GPSDestLongitude,
            Tag.GPSDestLongitudeRef,
            Tag.GPSDifferential,
            Tag.GPSDOP,
            Tag.GPSImgDirection,
            Tag.GPSImgDirectionRef,
            Tag.GPSLatitude,
            Tag.GPSLatitudeRef,
            Tag.GPSLongitude,
            Tag.GPSLongitudeRef,
            Tag.GPSMapDatum,
            Tag.GPSMeasureMode,
            Tag.GPSProcessingMethod,
            Tag.GPSSatellites,
            Tag.GPSSpeed,
            Tag.GPSSpeedRef,
            Tag.GPSStatus,
            Tag.GPSTimeStamp,
            Tag.GPSTrack,
            Tag.GPSTrackRef,
            Tag.GPSVersionID,
            Tag.FilterLookupTableDescription,
            Tag.FindingsGroupRecordingDateTrial,
            Tag.FindingsGroupRecordingTimeTrial,
            Tag.FixationDeviceDescription,
            Tag.EntityName,
            Tag.EquipmentFrameOfReferenceDescription,
            Tag.DoseReferenceDescription,
            Tag.DisplacementReferenceLabel,
            Tag.CurveData,
            Tag.ContainerComponentID,
            Tag.ContainerDescription,
            Tag.ClinicalTrialTimePointDescription,
            Tag.ClinicalTrialSeriesDescription,
            Tag.ClinicalTrialSeriesID,
            Tag.ClinicalTrialProtocolEthicsCommitteeApprovalNumber,
            Tag.CameraOwnerName,
            Tag.CompensatorDescription,
            Tag.BolusDescription,
            Tag.BarcodeValue,
            Tag.BeamDescription,
            Tag.AffectedSOPInstanceUID,
            Tag.AnnotationGroupDescription,
            Tag.CommentsOnRadiationDose,
            Tag.DeviceDescription,
            Tag.DeviceSettingDescription,
            Tag.FractionGroupDescription,
            Tag.PositionAcquisitionTemplateDescription,
            Tag.PositionAcquisitionTemplateName,
            Tag.DecompositionDescription,
            //Missing in part 15
            Tag.IdentifyingPrivateElements,
            Tag.PrivateDataElement,
            Tag.PrivateDataElementValueMultiplicity,
            Tag.PrivateDataElementValueRepresentation,
            Tag.PrivateDataElementNumberOfItems,
            Tag.PrivateDataElementName,
            Tag.PrivateDataElementKeyword,
            Tag.PrivateDataElementDescription,
            Tag.PrivateDataElementEncoding,
            Tag.TypeOfPatientID,
            Tag.PatientBirthDateInAlternativeCalendar,
            Tag.PatientDeathDateInAlternativeCalendar,
            Tag.PatientAlternativeCalendar,
            Tag.PatientSpeciesDescription,
            Tag.PatientSpeciesCodeSequence,
            Tag.PatientBreedDescription,
            Tag.PatientBreedCodeSequence,
            Tag.NamesOfIntendedRecipientsOfResults,
            Tag.DocumentingObserverIdentifierCodeSequenceTrial,
            Tag.EntityDescription,
            Tag.ContentSequence,
            Tag.PersonIdentificationCodeSequence,
            Tag.VerifyingObserverSequence,
            Tag.GraphicAnnotationSequence,
            Tag.FlowIdentifierSequence,
            Tag.PrescriptionNotesSequence

    };
    private static final int[] X_INSTITUTION = {
            Tag.InstitutionAddress,
            Tag.InstitutionalDepartmentName,
            Tag.InstitutionalDepartmentTypeCodeSequence
    };
    //End Of X
    private static final int[] X_DEVICE = {
            Tag.CassetteID,
            Tag.GantryID,
            Tag.GeneratorID,
            Tag.PerformedStationAETitle,
            Tag.PerformedStationGeographicLocationCodeSequence,
            Tag.PerformedStationName,
            Tag.PerformedStationNameCodeSequence,
            Tag.PlateID,
            Tag.ScheduledProcedureStepLocation,
            Tag.ScheduledStationAETitle,
            Tag.ScheduledStationGeographicLocationCodeSequence,
            Tag.ScheduledStationName,
            Tag.ScheduledStationNameCodeSequence,
            Tag.ScheduledStudyLocation,
            Tag.ScheduledStudyLocationAETitle,
            Tag.SourceSerialNumber,
    };
    private static final int[] X_DATES = {
            Tag.CurveDate,
            Tag.CurveTime,
            Tag.ExpectedCompletionDateTime,
            Tag.InstanceCoercionDateTime,
            Tag.InstanceCreationDate, // missing in Part 15
            Tag.InstanceCreationTime, // missing in Part 15
            Tag.LastMenstrualDate,
            Tag.ObservationDateTime,
            Tag.ObservationDateTrial,
            Tag.ObservationTimeTrial,
            Tag.OverlayDate,
            Tag.OverlayTime,
            Tag.PerformedProcedureStepEndDate,
            Tag.PerformedProcedureStepEndDateTime,
            Tag.PerformedProcedureStepEndTime,
            Tag.PerformedProcedureStepStartDate,
            Tag.PerformedProcedureStepStartDateTime,
            Tag.PerformedProcedureStepStartTime,
            Tag.ProcedureStepCancellationDateTime,
            Tag.ScheduledProcedureStepEndDate,
            Tag.ScheduledProcedureStepEndTime,
            Tag.ScheduledProcedureStepModificationDateTime,
            Tag.ScheduledProcedureStepStartDate,
            Tag.ScheduledProcedureStepStartDateTime,
            Tag.ScheduledProcedureStepStartTime,
            Tag.TimezoneOffsetFromUTC,
            //InselGruppe
            Tag.TreatmentDate,
            Tag.TreatmentTime,
            Tag.StudyVerifiedDate,
            Tag.StudyVerifiedTime,
            Tag.SubstanceAdministrationDateTime,
            Tag.StudyReadDate,
            Tag.StudyReadTime,
            Tag.StudyCompletionDate,
            Tag.StudyCompletionTime,
            Tag.StudyArrivalDate,
            Tag.StudyArrivalTime,
            Tag.SeriesTime,
            Tag.SeriesDate,
            Tag.ScheduledStudyStartDate,
            Tag.ScheduledStudyStartTime,
            Tag.ScheduledStudyStopDate,
            Tag.ScheduledStudyStopTime,
            Tag.ScheduledProcedureStepExpirationDateTime,
            Tag.ScheduledAdmissionDate,
            Tag.ScheduledAdmissionTime,
            Tag.ScheduledDischargeDate,
            Tag.ScheduledDischargeTime,
            Tag.RTPlanTime,
            Tag.RTPlanDate,
            Tag.RadiopharmaceuticalStartDateTime,
            Tag.RadiopharmaceuticalStartTime,
            Tag.RadiopharmaceuticalStopDateTime,
            Tag.RadiopharmaceuticalStopTime,
            Tag.ProductExpirationDateTime,
            Tag.PresentationCreationDate,
            Tag.PresentationCreationTime,
            Tag.ObservationStartDateTime,
            Tag.ModifiedImageTime,
            Tag.ModifiedImageDate,
            Tag.InterpretationTranscriptionDate,
            Tag.InterpretationTranscriptionTime,
            Tag.InterventionDrugStartTime,
            Tag.InterventionDrugStopTime,
            Tag.InterpretationRecordedDate,
            Tag.InterpretationRecordedTime,
            Tag.InterpretationApprovalDate,
            Tag.InterpretationApprovalTime,
            Tag.IntendedFractionStartTime,
            Tag.IntendedPhaseEndDate,
            Tag.IntendedPhaseStartDate,
            Tag.HL7DocumentEffectiveTime,
            Tag.FirstTreatmentDate,
            Tag.EthicsCommitteeApprovalEffectivenessEndDate,
            Tag.EthicsCommitteeApprovalEffectivenessStartDate,
            Tag.DischargeTime,
            Tag.DischargeDate,
            Tag.DateOfDocumentOrVerbalTransactionTrial,
            Tag.DateOfLastCalibration,
            Tag.DateOfLastDetectorCalibration,
            Tag.DateOfSecondaryCapture,
            Tag.DateTimeOfLastCalibration,
            Tag.CreationDate,
            Tag.CreationTime,
            Tag.ContrastBolusStartTime,
            Tag.ContrastBolusStopTime,
            Tag.ContributionDateTime,
            Tag.CertifiedTimestamp,
            Tag.CalibrationTime,
            Tag.CalibrationDate,
            Tag.AssertionExpirationDateTime,
            Tag.ApprovalStatusDateTime,
            Tag.AdmittingTime,
            Tag.AdmittingDate,


    };
    private static final int[] Z = {
            Tag.AccessionNumber,
            Tag.ConsultingPhysicianName,
            Tag.ContentCreatorName,
            Tag.FillerOrderNumberImagingServiceRequest,
            Tag.PatientID,
            Tag.PatientSexNeutered,
            Tag.PatientBirthDate,
            Tag.PatientName,
            Tag.PatientSex,
            Tag.PlacerOrderNumberImagingServiceRequest,
            Tag.ReferringPhysicianName,
            Tag.RequestedProcedureDescription,
            Tag.ReviewerName,
            Tag.StudyID,
            Tag.VerifyingObserverIdentificationCodeSequence,
            //InselGruppe
            Tag.SpecimenPreparationSequence,
            Tag.SourceOfPreviousValues,
            Tag.RTAccessoryDeviceSlotID,
            Tag.RTAccessoryHolderSlotID,
            Tag.RTPhysicianIntentNarrative,
            Tag.ROIInterpreter,
            Tag.ROIName,
            Tag.ReasonForSuperseding,
            Tag.RadiationGenerationModeDescription,
            Tag.ManufacturerDeviceIdentifier,
            Tag.IssuerOfTheContainerIdentifierSequence,
            Tag.IssuerOfTheSpecimenIdentifierSequence,
            Tag.ContrastBolusAgent,
            Tag.ClinicalTrialCoordinatingCenterName,
            Tag.ClinicalTrialProtocolName,
            Tag.ClinicalTrialTimePointID,
            Tag.ConceptualVolumeCombinationDescription,
            Tag.ConceptualVolumeDescription,
            Tag.ClinicalTrialSiteID,
            Tag.ClinicalTrialSiteName,
            Tag.FractionationNotes,
            Tag.DeviceAlternateIdentifier,
            Tag.PrescriptionNotes,
            Tag.TreatmentTechniqueNotes

    };
    //End of X-Dates
    private static final int[] Z_INSTITUTION = {
            Tag.InstitutionCodeSequence
    };
    //End of Z
    private static final int[] Z_DATES = {
            Tag.AcquisitionDate,
            Tag.AcquisitionTime,
            Tag.AdmittingTime,
            Tag.StudyDate,
            Tag.StudyTime,
            //InselGruppe
            Tag.StructureSetTime,
            Tag.StructureSetDate,
            Tag.ReviewDate,
            Tag.ReviewTime,
            Tag.ParticipationDateTime,
            Tag.InstructionPerformedDateTime,
            Tag.CalibrationDateTime,
    };
    private static final int[] Z_UID = {
            Tag.ReferencedPerformedProcedureStepSequence,
            Tag.ReferencedStudySequence
    };
    private static final int[] D = {
            Tag.AcquisitionDeviceProcessingDescription,
            Tag.OperatorsName,
            Tag.PersonName,
            Tag.ProtocolName,
            Tag.VerifyingObserverName,
            Tag.VerifyingOrganization,
            //Inselgruppe
            Tag.XRayDetectorID,
            Tag.XRaySourceID,
            Tag.UserContentLongLabel,
            Tag.UserContentLabel,
            Tag.TreatmentToleranceViolationDateTime,
            Tag.TreatmentToleranceViolationDescription,
            Tag.TreatmentSite,
            Tag.TreatmentPositionGroupLabel,
            Tag.StructureSetLabel,
            Tag.SpecimenIdentifier,
            Tag.SourceIdentifier,
            Tag.SelectorAEValue,
            Tag.SelectorASValue,
            Tag.SelectorLOValue,
            Tag.SelectorLTValue,
            Tag.SelectorOBValue,
            Tag.SelectorPNValue,
            Tag.SelectorSHValue,
            Tag.SelectorSTValue,
            Tag.SelectorUNValue,
            Tag.SelectorURValue,
            Tag.SelectorUTValue,
            Tag.RTPrescriptionLabel,
            Tag.RTToleranceSetLabel,
            Tag.RTPlanLabel,
            Tag.ReasonForTheAttributeModification,
            Tag.RadiationGenerationModeLabel,
            Tag.RadiationDoseIdentificationLabel,
            Tag.RadiationDoseInVivoMeasurementLabel,
            Tag.ModifyingSystem,
            Tag.InterlockDescription,
            Tag.InterlockOriginDescription,
            Tag.FrameOriginTimestamp,
            Tag.FlowIdentifier,
            Tag.EntityLongLabel,
            Tag.EncapsulatedDocument,
            Tag.ContainerIdentifier,
            Tag.CertificateOfSigner,
            Tag.ClinicalTrialProtocolEthicsCommitteeName,
            Tag.ClinicalTrialProtocolID,
            Tag.ClinicalTrialSponsorName,
            Tag.ClinicalTrialSubjectID,
            Tag.ClinicalTrialSubjectReadingID,
            Tag.AcquisitionFieldOfViewLabel,
            Tag.EntityLabel,
            Tag.DestinationAE,
            Tag.AnnotationGroupLabel,
    };
    private static final int[] D_DEVICE = {
            Tag.DetectorID,
            Tag.DeviceSerialNumber,
            Tag.StationName,
    };
    //End of D
    private static final int[] D_INSTITUTION = {
            Tag.InstitutionName,
    };
    private static final int[] D_DATES = {
            Tag.AcquisitionDateTime,
            Tag.ContentDate,
            Tag.ContentTime,
            Tag.EndAcquisitionDateTime,
            Tag.StartAcquisitionDateTime,
            Tag.VerificationDateTime, // missing in Part 15
            //InselGruppe
            Tag.TreatmentControlPointDate,
            Tag.TreatmentControlPointTime,
            Tag.Time,
            Tag.Date,
            Tag.SourceStartDateTime,
            Tag.SourceStrengthReferenceDate,
            Tag.SourceStrengthReferenceTime,
            Tag.SourceEndDateTime,
            Tag.SafePositionExitDate,
            Tag.SafePositionExitTime,
            Tag.SafePositionReturnDate,
            Tag.SafePositionReturnTime,
            Tag.ReferencedDateTime,
            Tag.RecordedRTControlPointDateTime,
            Tag.OverrideDateTime,
            Tag.InterlockDateTime,
            Tag.ImpedanceMeasurementDateTime,
            Tag.InformationIssueDateTime,
            Tag.HangingProtocolCreationDateTime,
            Tag.FrameReferenceDateTime,
            Tag.FrameAcquisitionDateTime,
            Tag.EffectiveDateTime,
            Tag.ExclusionStartDateTime,
            Tag.DateTime,
            Tag.DigitalSignatureDateTime,
            Tag.DeviceLabel,
            Tag.DecayCorrectionDateTime,
            Tag.AttributeModificationDateTime,
            Tag.AssertionDateTime,
            Tag.BeamHoldTransitionDateTime,
            Tag.SelectorTMValue,
            Tag.SelectorDAValue,
            Tag.SelectorDTValue,
            Tag.FunctionalSyncPulse,
            Tag.ContextGroupLocalVersion,
            Tag.ContextGroupVersion,
    };
    private static final int[] U = {
            Tag.ConcatenationUID,
            Tag.DimensionOrganizationUID,
            Tag.FailedSOPInstanceUIDList,
            Tag.FiducialUID,
            Tag.FrameOfReferenceUID,
            Tag.InstanceCreatorUID,
            Tag.IrradiationEventUID,
            Tag.LargePaletteColorLookupTableUID,
            Tag.MediaStorageSOPInstanceUID,
            Tag.ObservationSubjectUIDTrial,
            Tag.ObservationUID,
            Tag.PaletteColorLookupTableUID,
            Tag.PresentationDisplayCollectionUID,
            Tag.PresentationSequenceCollectionUID,
            Tag.ReferencedFrameOfReferenceUID,
            Tag.ReferencedGeneralPurposeScheduledProcedureStepTransactionUID,
            Tag.ReferencedObservationUIDTrial,
            Tag.ReferencedSOPInstanceUID,
            Tag.ReferencedSOPInstanceUIDInFile,
            Tag.RelatedFrameOfReferenceUID,
            Tag.RequestedSOPInstanceUID,
            Tag.SeriesInstanceUID,
            Tag.SOPInstanceUID,
            Tag.StorageMediaFileSetUID,
            Tag.StudyInstanceUID,
            Tag.SynchronizationFrameOfReferenceUID,
            Tag.TargetUID,
            Tag.TemplateExtensionCreatorUID,
            Tag.TemplateExtensionOrganizationUID,
            Tag.TrackingUID,
            Tag.TransactionUID,
            Tag.UID,
            //InselGruppe
            Tag.DoseReferenceUID,
            Tag.TreatmentPositionGroupUID,
            Tag.TreatmentSessionUID,
            Tag.SpecimenUID,
            Tag.SourceFrameOfReferenceUID,
            Tag.SourceConceptualVolumeUID,
            Tag.RTTreatmentPhaseUID,
            Tag.ReferencedTreatmentPositionGroupUID,
            Tag.ReferencedDoseReferenceUID,
            Tag.ReferencedDosimetricObjectiveUID,
            Tag.ReferencedFiducialsUID,
            Tag.ReferencedConceptualVolumeUID,
            Tag.PyramidUID,
            Tag.PatientSetupUID,
            Tag.MultiplexGroupUID,
            Tag.ManufacturerDeviceClassUID,
            Tag.DosimetricObjectiveUID,
            Tag.DigitalSignatureUID,
            Tag.ConceptualVolumeUID,
            Tag.ConstituentConceptualVolumeUID,
            Tag.AcquisitionUID,
            Tag.AnnotationGroupUID

    };
    private static final int[] U_DEVICE = {
            Tag.DeviceUID,
    };
    private static final String UNMODIFIED = "UNMODIFIED";
    private static final String REMOVED = "REMOVED";
    private static final String YES = "YES";
    private static final ElementDictionary dict = ElementDictionary.getStandardElementDictionary();
    private final EnumSet<Option> options;
    private final Attributes dummyValues = new Attributes();
    private final int[] o;
    private int[] x = X;
    private int[] u = U;

    public DcmDeIdentifier(Option... options) {
        this.options = EnumSet.of(Option.BasicApplicationConfidentialityProfile, options);
        int[] z = Z;
        int[] d = D;
        if (!this.options.contains(Option.RetainDeviceIdentityOption)) {
            x = cat(x, X_DEVICE);
            d = cat(d, D_DEVICE);
            u = cat(u, U_DEVICE);
        }
        if (!this.options.contains(Option.RetainInstitutionIdentityOption)) {
            x = cat(x, X_INSTITUTION);
            z = cat(z, Z_INSTITUTION);
            d = cat(d, D_INSTITUTION);
        }
        if (!this.options.contains(Option.RetainLongitudinalTemporalInformationFullDatesOption)) {
            x = cat(x, X_DATES);
            z = cat(z, Z_DATES);
            d = cat(d, D_DATES);
        }
        if (!this.options.contains(Option.RetainUIDsOption)) {
            z = cat(z, Z_UID);
        }
        o = cat(z, d);
        Arrays.sort(x);
        Arrays.sort(u);
        Arrays.sort(o);
        initDummyValues(d);
    }

    private static String hash(IDWithIssuer pid) {
        return UUID.nameUUIDFromBytes(pid.toString().getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static int[] cat(int[] a, int[] b) {
        int[] dest = new int[a.length + b.length];
        System.arraycopy(a, 0, dest, 0, a.length);
        System.arraycopy(b, 0, dest, a.length, b.length);
        return dest;
    }

    private static String dummyValueFor(VR vr) {

        switch (vr) {
            case DA:
                return "19991111";
            case DT:
                return "19991111111111";
            case TM:
                return "111111";
            case IS:
                return "0";
            case OB:
                return "";
            case DS:
                return "0";
            case US:
                return "0";
            case AS:
                return "0";
            case UL:
                return "0";
            case UN:
                return null;
            case PN:
        }
        return "REMOVED";
    }

    public void setDummyValue(int tag, VR vr, String s) {
        dummyValues.setString(tag, vr, s);
    }

    public void deidentify(Attributes attrs, final List<Deidentify.AuxTag> retainTagsList) {
        IDWithIssuer pid = options.contains(Option.RetainPatientIDHashOption) ? IDWithIssuer.pidOf(attrs) : null;
        deidentifyItem(attrs, retainTagsList);
        correct(attrs);
        if (pid != null) attrs.setString(Tag.PatientID, VR.LO, hash(pid));
        attrs.setString(Tag.PatientIdentityRemoved, VR.CS, YES);
        attrs.setString(Tag.LongitudinalTemporalInformationModified, VR.CS,
                options.contains(Option.RetainLongitudinalTemporalInformationFullDatesOption) ? UNMODIFIED : REMOVED);
        Sequence sq = attrs.ensureSequence(Tag.DeidentificationMethodCodeSequence, options.size());
        List<String> allMeanings = new ArrayList<>();
        for (Option option : options) {
            sq.add(option.code.toItem());
            allMeanings.add(option.code.getCodeMeaning());
        }
        //Recommended
        attrs.setString(Tag.DeidentificationMethod, VR.LO, allMeanings.toArray(new String[0]));
    }

    public String remapUID(String uid) {
        return options.contains(Option.RetainUIDsOption) ? uid : UIDUtils.remapUID(uid);
    }

    private boolean equalOptions(Option... options) {
        return EnumSet.of(Option.BasicApplicationConfidentialityProfile, options).equals(options);
    }

    private void initDummyValues(int[] d) {
        ElementDictionary dict = ElementDictionary.getStandardElementDictionary();
        for (int tag : d) {
            initDummyValue(dict.vrOf(tag), tag);
        }
        initDummyValue(VR.DA, Tag.SeriesDate);
        initDummyValue(VR.TM, Tag.SeriesTime);
    }

    private void initDummyValue(VR vr, int tag) {
        String dummy = dummyValueFor(vr);
        if (null != dummy) {
            log.debug("Set Dummy Value For Tag:{}, VR:{}, newVR:{}, Dummy:{}", dict.keywordOf(tag), dict.vrOf(tag), vr, dummy);
            dummyValues.setString(tag, vr, dummy);
        } else {
            log.info("No Dummy Value For Tag:{}, VR:{}, newVR:{}, Dummy:'{}'", dict.keywordOf(tag), dict.vrOf(tag), vr, dummy);
        }
    }

    private void correct(Attributes attrs) {
        if (!options.contains(Option.RetainLongitudinalTemporalInformationFullDatesOption)
                && UID.PositronEmissionTomographyImageStorage.equals(attrs.getString(Tag.SOPClassUID))) {
            attrs.setString(Tag.SeriesDate, VR.DA, dummyValues.getString(Tag.SeriesDate));
            attrs.setString(Tag.SeriesTime, VR.TM, dummyValues.getString(Tag.SeriesTime));
        }
    }

    /**
     * @param attrs          Sequence elements are anonymized by using
     *                       this same table of attributes for each
     *                       element in the sequence.
     *                       Tag.ContentSequence,
     *                       Tag.PersonIdentificationCodeSequence,
     *                       Tag.VerifyingObserverSequence,
     *                       Tag.GraphicAnnotationSequence,
     *                       Tag.FlowIdentifierSequence,
     *                       will be visited
     * @param retainTagsList
     */
    private void deidentifyItem(Attributes attrs, List<Deidentify.AuxTag> retainTagsList) {

        Map<Integer,Object> valuesRetain = new HashMap<>();
        retainTagsList.forEach(tag -> {
            if (tag.getVr() == VR.SQ){
                Sequence sequence = attrs.getSequence(tag.getTag());
                valuesRetain.put(tag.getTag(), sequence);
            }else{
                String valueStr = attrs.getString(tag.getTag());
                valuesRetain.put(tag.getTag(), valueStr);
            }
        });

        attrs.removePrivateAttributes();
        attrs.removeCurveData();
        attrs.removeOverlayData();
        attrs.removeSelected(x);
        attrs.replaceSelected(dummyValues, o);
        if (!options.contains(Option.RetainUIDsOption)) {
            attrs.replaceUIDSelected(u);
        }

        try {
            attrs.accept(new Attributes.Visitor() {
                @Override
                public boolean visit(Attributes attrs, int tag, VR vr, Object value) throws Exception {
                    if (value instanceof Sequence)
                        for (Attributes item : (Sequence) value)
                            deidentifyItem(item, retainTagsList);
                    return true;
                }
            }, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        valuesRetain.forEach((tag,value) -> {
            VR vr = ElementDictionary.getStandardElementDictionary().vrOf(tag);
            if (vr == VR.SQ) {
                Sequence originalSeq = (Sequence) value;
                if (originalSeq != null) {
                    Sequence newSeq = attrs.newSequence(tag, originalSeq.size());
                    for (Attributes item : originalSeq) {
                        newSeq.add(new Attributes(item)); // Deep copy
                    }
                }
            } else {
                String s = (String) value;
                if (s != null) {
                    attrs.setString(tag, vr, s);
                }
            }
        });
    }

    public enum Option {
        BasicApplicationConfidentialityProfile(DeIdentificationMethod.BasicApplicationConfidentialityProfile),
        //        CleanPixelDataOption(DeIdentificationMethod.CleanPixelDataOption),
//        CleanRecognizableVisualFeaturesOption(DeIdentificationMethod.CleanRecognizableVisualFeaturesOption),
//        CleanGraphicsOption(DeIdentificationMethod.CleanGraphicsOption),
//        CleanStructuredContentOption(DeIdentificationMethod.CleanStructuredContentOption),
//        CleanDescriptorsOption(DeIdentificationMethod.CleanDescriptorsOption),
        RetainLongitudinalTemporalInformationFullDatesOption(
                DeIdentificationMethod.RetainLongitudinalTemporalInformationFullDatesOption),
        //        RetainLongitudinalTemporalInformationModifiedDatesOption(
//                DeIdentificationMethod.RetainLongitudinalTemporalInformationModifiedDatesOption),
//        RetainPatientCharacteristicsOption(DeIdentificationMethod.RetainPatientCharacteristicsOption),
        RetainDeviceIdentityOption(DeIdentificationMethod.RetainDeviceIdentityOption),
        RetainInstitutionIdentityOption(DeIdentificationMethod.RetainInstitutionIdentityOption),
        RetainUIDsOption(DeIdentificationMethod.RetainUIDsOption),
        //        RetainSafePrivateOption(DeIdentificationMethod.RetainSafePrivateOption),
        RetainPatientIDHashOption(DeIdentificationMethod.RetainPatientIDHashOption);

        private final Code code;

        Option(Code code) {
            this.code = code;
        }
    }

}