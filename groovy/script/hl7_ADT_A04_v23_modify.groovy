import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import ca.uhn.hl7v2.model.v23.segment.EVN;
import ca.uhn.hl7v2.model.v23.message.ADT_A04;

def flowFile = session.get()
if (!flowFile) {
    return
}

HapiContext context = new DefaultHapiContext();
PipeParser parser = context.getPipeParser();
context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
context.getParserConfiguration().setValidating(false);
Message hapiMsg = parser.parse(flowFile.read().getText('UTF-8'));

//Only selected messages
if (! hapiMsg instanceof ca.uhn.hl7v2.model.v23.message.ADT_A04){
    REL_FAILURE << flowFile
    return
}

ADT_A04 adtA04 = (ADT_A04) hapiMsg;
EVN evn = adtA04.getEVN();
evn.getDateTimePlannedEvent().getTimeOfAnEvent().setValue( "20250116000000");
evn.getEvn2_RecordedDateTime().getTimeOfAnEvent().setValue("20250108000000");

final String encodedMessage = parser.encode(hapiMsg);

flowFile.write("UTF-8"){ out ->
    out << encodedMessage
}
REL_SUCCESS << flowFile