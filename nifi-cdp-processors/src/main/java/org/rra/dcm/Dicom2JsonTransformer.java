package org.rra.dcm;

import jakarta.json.Json;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;
import org.apache.commons.io.IOUtils;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.BasicBulkDataDescriptor;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Dicom2JsonTransformer {


    private Dicom2JsonTransformer() {
    }

    public static void transform(InputStream in, OutputStream out, Boolean includeBulkData, boolean indent, boolean tagNames, boolean removePrivateAttributes, boolean encodeAsNumber) throws IOException {
        if (removePrivateAttributes){
            try(ByteArrayOutputStream bas = new ByteArrayOutputStream()){
                try(BufferedOutputStream bos = new BufferedOutputStream(bas)) {
                    removePrivateAttributes(in, bos);
                }
                try(ByteArrayInputStream bais = new ByteArrayInputStream(bas.toByteArray())){
                    try(BufferedInputStream bis = new BufferedInputStream(bais)) {
                        DicomInputStream dis = new DicomInputStream(bis);
                        try {
                            parse(dis, out, includeBulkData, indent, tagNames, removePrivateAttributes, encodeAsNumber);
                        } catch (Exception exception) {
                            throw exception;
                        } finally {
                            dis.close();
                        }
                    }
                }
            }
        }else{
            DicomInputStream dis = new DicomInputStream(in);
            try {
                parse(dis, out, includeBulkData, indent, tagNames, removePrivateAttributes, encodeAsNumber);
            } catch (Exception exception) {
                throw exception;
            } finally {
                dis.close();
            }
        }
    }
    private static void removePrivateAttributes(InputStream in, OutputStream out) throws IOException {
        try (BufferedInputStream bif = new BufferedInputStream(in)) {
            DicomDataReader data = new DicomDataReader(bif, true);
            Attributes dcm = data.getAttributes();
            dcm.removePrivateAttributes();
            Attributes fmi = data.getFmi();
            if (null == fmi) {
                fmi = dcm.createFileMetaInformation(UID.ExplicitVRLittleEndian);
            }
            try (DicomOutputStream dos = new DicomOutputStream(out, UID.ExplicitVRLittleEndian)) {
                dos.setEncodingOptions(DicomEncodingOptions.DEFAULT);
                dos.writeDataset(fmi, dcm);
            }
        }
    }
    private static void parse(DicomInputStream dis, OutputStream out, Boolean includeBulkData, boolean indent, boolean tagNames, boolean removePrivateAttributes, boolean encodeAsNumber) throws IOException {
        BasicBulkDataDescriptor bulkDataDescriptor = new BasicBulkDataDescriptor();
        bulkDataDescriptor.excludeDefaults(false);
        if (null == includeBulkData) {
            dis.setIncludeBulkData(DicomInputStream.IncludeBulkData.URI);
        } else if (includeBulkData) {
            dis.setIncludeBulkData(DicomInputStream.IncludeBulkData.YES);
        } else {
            dis.setIncludeBulkData(DicomInputStream.IncludeBulkData.NO);
        }
        dis.setBulkDataDescriptor(bulkDataDescriptor);
        dis.setBulkDataDirectory(null);
        dis.setBulkDataFilePrefix("blk");
        dis.setBulkDataFileSuffix(null);
        dis.setConcatenateBulkDataFiles(false);
        JsonGenerator jsonGen = createGenerator(out, indent);

        JSONTagNameWriter jsonWriter = new JSONTagNameWriter(jsonGen, tagNames);
        if (encodeAsNumber) {
            jsonWriter.setJsonType(VR.DS, JsonValue.ValueType.NUMBER);
            jsonWriter.setJsonType(VR.IS, JsonValue.ValueType.NUMBER);
            jsonWriter.setJsonType(VR.SV, JsonValue.ValueType.NUMBER);
            jsonWriter.setJsonType(VR.UV, JsonValue.ValueType.NUMBER);
        }
        dis.setDicomInputHandler(jsonWriter);

        dis.readDataset();
        jsonGen.flush();
    }

    private static JsonGenerator createGenerator(OutputStream out, boolean indent) {
        Map<String, ?> conf = new HashMap<>(2);
        if (indent)
            conf.put(JsonGenerator.PRETTY_PRINTING, null);
        return Json.createGeneratorFactory(conf).createGenerator(out);
    }
}

