package edu.uci.ics.asterix.external.library.utils;

import org.trec.kba.streamcorpus.StreamItem;

import edu.uci.ics.asterix.external.dataset.adapter.KBAStreamDocument;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;


public class KBAStreamItemProcessor extends KBAStreamDocument {

    private IAObject[] mutableKBAStreamDocumentFields;
    private AMutableRecord mutableRecord;
   
        

    
    public KBAStreamItemProcessor(ARecordType recordType ) {
        super();
        mutableKBAStreamDocumentFields = new AMutableString[fieldNames.length];
        for (int i=0; i<fieldNames.length; i++) {
            mutableKBAStreamDocumentFields[i] = new AMutableString(null);
        }
        mutableRecord = new AMutableRecord(recordType, mutableKBAStreamDocumentFields); 
    }
    

    
    public AMutableRecord processNextStreamDocument(StreamItem si, String dirName) {
        initialize(si, dirName, null);
        for (int i=0; i<fieldNames.length; i++) {
            ((AMutableString) mutableKBAStreamDocumentFields[i]).setValue(fields.get(fieldNames[i]));
        }

        for (int i = 0; i < mutableKBAStreamDocumentFields.length; i++) {
            mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
        }

        return mutableRecord;

    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

}