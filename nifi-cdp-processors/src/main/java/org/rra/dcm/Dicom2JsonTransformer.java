package org.rra.dcm;

import jakarta.json.Json;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.BasicBulkDataDescriptor;
import org.dcm4che3.io.DicomInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class Dicom2JsonTransformer {
    private static final Boolean ENCODE_AS_NUMBER = Boolean.FALSE;

    private Dicom2JsonTransformer() {
    }

    public static void transform(InputStream in, OutputStream out, Boolean includeBulkData, boolean indent, boolean tagNames) throws IOException {
        DicomInputStream dis = new DicomInputStream(in);
        try {
            parse(dis, out, includeBulkData, indent, tagNames);
        } catch (Exception exception) {
            throw exception;
        } finally {
            dis.close();
        }
    }

    private static void parse(DicomInputStream dis, OutputStream out, Boolean includeBulkData, boolean indent, boolean tagNames) throws IOException {
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
        if (ENCODE_AS_NUMBER) {
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

