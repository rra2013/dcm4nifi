package org.rra.dcm;

import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;
import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.*;
import org.dcm4che3.data.PersonName.Group;
import org.dcm4che3.io.DicomInputHandler;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.util.Base64;
import org.dcm4che3.util.StringUtils;
import org.dcm4che3.util.TagUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.function.LongFunction;

@Slf4j
public class JSONTagNameWriter implements DicomInputHandler {

    private static final int DOUBLE_MAX_BITS = 53;

    private final JsonGenerator gen;
    private final Deque<Boolean> hasItems = new ArrayDeque<>();
    private final boolean printTagNames;
    private final ElementDictionary dict = ElementDictionary.getStandardElementDictionary();
    private final boolean removePrivateAttributes;
    private String replaceBulkDataURI;
    private final EnumMap<VR, JsonValue.ValueType> jsonTypeByVR = new EnumMap<>(VR.class);


    public JSONTagNameWriter(JsonGenerator gen, boolean printTagNames, boolean removePrivateAttributes) {
        this.gen = gen;
        this.printTagNames = printTagNames;
        this.removePrivateAttributes = removePrivateAttributes;
    }

    private static VR requireIS_DS_SV_UV(VR vr) {
        if (vr != VR.DS && vr != VR.IS && vr != VR.SV && vr != VR.UV)
            throw new IllegalArgumentException("vr:" + vr);
        return vr;
    }

    private static JsonValue.ValueType requireNumberOrString(JsonValue.ValueType jsonType) {
        if (jsonType != JsonValue.ValueType.NUMBER && jsonType != JsonValue.ValueType.STRING)
            throw new IllegalArgumentException("jsonType:" + jsonType);
        return jsonType;
    }

    public void setJsonType(VR vr, JsonValue.ValueType valueType) {
        jsonTypeByVR.put(requireIS_DS_SV_UV(vr), requireNumberOrString(valueType));
    }

    public String getReplaceBulkDataURI() {
        return replaceBulkDataURI;
    }

    public void setReplaceBulkDataURI(String replaceBulkDataURI) {
        this.replaceBulkDataURI = replaceBulkDataURI;
    }

    /**
     * Writes the given attributes as a full JSON object. Subsequent calls will generate a new JSON
     * object.
     */
    public void write(Attributes attrs) {
        gen.writeStartObject();
        writeAttributes(attrs);
        gen.writeEnd();
    }

    /**
     * Writes the given attributes to JSON. Can be used to output multiple attributes (e.g. metadata,
     * attributes) to the same JSON object.
     */
    public void writeAttributes(Attributes attrs) {
        final SpecificCharacterSet cs = attrs.getSpecificCharacterSet();
        try {
            attrs.accept(new Attributes.Visitor() {
                             @Override
                             public boolean visit(Attributes attrs, int tag, VR vr, Object value)
                                     throws Exception {
                                 writeAttribute(tag, vr, value, cs, attrs);
                                 return true;
                             }
                         },
                    false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writeAttribute(int tag, VR vr, Object value,
                                SpecificCharacterSet cs, Attributes attrs) {
        if (TagUtils.isGroupLength(tag))
            return;

        if (this.printTagNames) {
            String tagName = dict.keywordOf(tag);
            if (null == tagName || tagName.equals("")) {
                gen.writeStartObject(TagUtils.toHexString(tag));
            } else {
                gen.writeStartObject(tagName);
            }
        } else {
            gen.writeStartObject(TagUtils.toHexString(tag));
        }

        gen.write("vr", vr.name());
        if (value instanceof Value)
            writeValue((Value) value, attrs.bigEndian());
        else
            writeValue(vr, value, attrs.bigEndian(),
                    attrs.getSpecificCharacterSet(vr), true);
        gen.writeEnd();
    }

    private void writeValue(Value value, boolean bigEndian) {
        if (value.isEmpty())
            return;

        if (value instanceof Sequence) {
            gen.writeStartArray("Value");
            for (Attributes item : (Sequence) value) {
                write(item);
            }
            gen.writeEnd();
        } else if (value instanceof Fragments frags) {
            gen.writeStartArray("DataFragment");
            for (Object frag : frags) {
                if (frag instanceof Value && ((Value) frag).isEmpty())
                    gen.writeNull();
                else {
                    gen.writeStartObject();
                    if (frag instanceof BulkData)
                        writeBulkData((BulkData) frag);
                    else {
                        writeInlineBinary(frags.vr(), (byte[]) frag, bigEndian, true);
                    }
                    gen.writeEnd();
                }
            }
            gen.writeEnd();
        } else if (value instanceof BulkData) {
            writeBulkData((BulkData) value);
        }
    }

    @Override
    public void readValue(DicomInputStream dis, Attributes attrs)
            throws IOException {
        int tag = dis.tag();
        VR vr = dis.vr();
        long len = dis.unsignedLength();
        if (TagUtils.isPrivateTag(tag) && removePrivateAttributes) {
            dis.readValue(dis, attrs);
            return;
        }
        if (TagUtils.isGroupLength(tag)) {
            dis.readValue(dis, attrs);
        } else if (dis.isExcludeBulkData()) {
            dis.readValue(dis, attrs);
        } else {
            if (this.printTagNames) {
                String tagName = dict.keywordOf(tag);
                if (null == tagName || tagName.equals("")) {
                    gen.writeStartObject(TagUtils.toHexString(tag));
                } else {
                    gen.writeStartObject(tagName);
                }
            }else{
                gen.writeStartObject(TagUtils.toHexString(tag));
            }
            gen.write("vr", vr.name());
            if (vr == VR.SQ || len == -1) {
                hasItems.addLast(false);
                dis.readValue(dis, attrs);
                if (hasItems.removeLast())
                    gen.writeEnd();
            } else if (len > 0) {
                if (dis.isIncludeBulkDataURI()) {
                    writeBulkData(dis.createBulkData(dis));
                } else {
                    byte[] b = dis.readValue();
                    if (tag == Tag.TransferSyntaxUID
                            || tag == Tag.SpecificCharacterSet
                            || tag == Tag.PixelRepresentation
                            || TagUtils.isPrivateCreator(tag))
                        attrs.setBytes(tag, vr, b);
                    writeValue(vr, b, dis.bigEndian(),
                            attrs.getSpecificCharacterSet(vr), false);
                }
            }
            gen.writeEnd();
        }
    }

    private void writeValue(VR vr, Object val, boolean bigEndian,
                            SpecificCharacterSet cs, boolean preserve) {
        switch (vr) {
            case AE:
            case AS:
            case AT:
            case CS:
            case DA:
            case DS:
            case DT:
            case IS:
            case LO:
            case LT:
            case PN:
            case SH:
            case ST:
            case TM:
            case UC:
            case UI:
            case UR:
            case UT:
                writeStringValues(vr, val, bigEndian, cs);
                break;
            case FL:
            case FD:
                writeDoubleValues(vr, val, bigEndian);
                break;
            case SL:
            case SS:
            case US:
                writeIntValues(vr, val, bigEndian);
                break;
            case SV:
                writeLongValues(Long::toString, vr, val, bigEndian);
                break;
            case UV:
                writeLongValues(Long::toUnsignedString, vr, val, bigEndian);
                break;
            case UL:
                writeUIntValues(vr, val, bigEndian);
                break;
            case OB:
            case OD:
            case OF:
            case OL:
            case OV:
            case OW:
            case UN:
                writeInlineBinary(vr, (byte[]) val, bigEndian, preserve);
                break;
            case SQ:
                assert true;
        }
    }

    private void writeStringValues(VR vr, Object val, boolean bigEndian,
                                   SpecificCharacterSet cs) {
        gen.writeStartArray("Value");
        Object o = vr.toStrings(val, bigEndian, cs);
        String[] ss = (o instanceof String[])
                ? (String[]) o
                : new String[]{(String) o};
        for (String s : ss) {
            if (s == null || s.isEmpty())
                gen.writeNull();
            else switch (vr) {
                case DS:
                    if (jsonTypeByVR.get(VR.DS) == JsonValue.ValueType.NUMBER) {
                        try {
                            gen.write(StringUtils.parseDS(s));
                        } catch (NumberFormatException e) {
                            log.info("illegal DS value: {} - encoded as string", s);
                            gen.write(s);
                        }
                    } else {
                        gen.write(s);
                    }
                    break;
                case IS:
                    if (jsonTypeByVR.get(VR.IS) == JsonValue.ValueType.NUMBER) {
                        writeNumber(s);
                    } else {
                        gen.write(s);
                    }
                    break;
                case PN:
                    writePersonName(s);
                    break;
                default:
                    gen.write(s);
            }
        }
        gen.writeEnd();
    }

    private void writeNumber(String s) {
        try {
            long l = StringUtils.parseIS(s);
            if ((l < 0 ? -l : l) >> DOUBLE_MAX_BITS == 0) {
                gen.write(l);
                return;
            }
        } catch (NumberFormatException e) {
            log.info("illegal IS value: {} - encoded as string", s);
        }
        gen.write(s);
    }

    private void writeDoubleValues(VR vr, Object val, boolean bigEndian) {
        gen.writeStartArray("Value");
        int vm = vr.vmOf(val);
        for (int i = 0; i < vm; i++) {
            double d = vr.toDouble(val, bigEndian, i, 0);
            if (Double.isNaN(d)) {
                log.info("encode {} NaN as null", vr);
                gen.writeNull();
            } else {
                if (d == Double.POSITIVE_INFINITY) {
                    d = Double.MAX_VALUE;
                    log.info("encode {} Infinity as {}", vr, d);
                } else if (d == Double.NEGATIVE_INFINITY) {
                    d = -Double.MAX_VALUE;
                    log.info("encode {} -Infinity as {}", vr, d);
                }
                gen.write(d);
            }
        }
        gen.writeEnd();
    }

    private void writeIntValues(VR vr, Object val, boolean bigEndian) {
        gen.writeStartArray("Value");
        int vm = vr.vmOf(val);
        for (int i = 0; i < vm; i++) {
            gen.write(vr.toInt(val, bigEndian, i, 0));
        }
        gen.writeEnd();
    }

    private void writeUIntValues(VR vr, Object val, boolean bigEndian) {
        gen.writeStartArray("Value");
        int vm = vr.vmOf(val);
        for (int i = 0; i < vm; i++) {
            gen.write(vr.toInt(val, bigEndian, i, 0) & 0xffffffffL);
        }
        gen.writeEnd();
    }

    private void writeLongValues(LongFunction<String> toString, VR vr, Object val, boolean bigEndian) {
        gen.writeStartArray("Value");
        boolean asString = jsonTypeByVR.get(vr) != JsonValue.ValueType.NUMBER;
        int vm = vr.vmOf(val);
        for (int i = 0; i < vm; i++) {
            long l = vr.toLong(val, bigEndian, i, 0);
            if (asString || (l < 0 ? (vr == VR.UV || (-l >> DOUBLE_MAX_BITS) > 0) : (l >> DOUBLE_MAX_BITS) > 0)) {
                gen.write(toString.apply(l));
            } else {
                gen.write(l);
            }
        }
        gen.writeEnd();
    }

    private void writePersonName(String s) {
        PersonName pn = new PersonName(s, true);
        gen.writeStartObject();
        writePNGroup("Alphabetic", pn, Group.Alphabetic);
        writePNGroup("Ideographic", pn, Group.Ideographic);
        writePNGroup("Phonetic", pn, Group.Phonetic);
        gen.writeEnd();
    }

    private void writePNGroup(String name, PersonName pn, Group group) {
        if (pn.contains(group))
            gen.write(name, pn.toString(group, true));
    }

    private void writeInlineBinary(VR vr, byte[] b, boolean bigEndian,
                                   boolean preserve) {
        if (bigEndian)
            b = vr.toggleEndian(b, preserve);
        gen.write("InlineBinary", encodeBase64(b));
    }

    private String encodeBase64(byte[] b) {
        int len = (b.length * 4 / 3 + 3) & ~3;
        char[] ch = new char[len];
        Base64.encode(b, 0, b.length, ch, 0);
        return new String(ch);
    }

    private void writeBulkData(BulkData blkdata) {
        gen.write("BulkDataURI", replaceBulkDataURI != null ? replaceBulkDataURI : blkdata.getURI());
    }

    @Override
    public void readValue(DicomInputStream dis, Sequence seq)
            throws IOException {
        if (!hasItems.getLast()) {
            gen.writeStartArray("Value");
            hasItems.removeLast();
            hasItems.addLast(true);
        }
        gen.writeStartObject();
        dis.readValue(dis, seq);
        gen.writeEnd();
    }

    @Override
    public void readValue(DicomInputStream dis, Fragments frags)
            throws IOException {
        int len = dis.length();
        if (dis.isExcludeBulkData()) {
            dis.skipFully(len);
            return;
        }
        if (!hasItems.getLast()) {
            gen.writeStartArray("DataFragment");
            hasItems.removeLast();
            hasItems.add(true);
        }

        if (len == 0)
            gen.writeNull();
        else {
            gen.writeStartObject();
            if (dis.isIncludeBulkDataURI()) {
                writeBulkData(dis.createBulkData(dis));
            } else {
                writeInlineBinary(frags.vr(), dis.readValue(),
                        dis.bigEndian(), false);
            }
            gen.writeEnd();
        }
    }

    @Override
    public void startDataset(DicomInputStream dis) throws IOException {
        gen.writeStartObject();
    }

    @Override
    public void endDataset(DicomInputStream dis) throws IOException {
        gen.writeEnd();
    }
}
