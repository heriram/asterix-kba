/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.library;

import java.util.List;

import edu.uci.ics.asterix.om.base.AMutableCircle;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableInterval;
import edu.uci.ics.asterix.om.base.AMutableLine;
import edu.uci.ics.asterix.om.base.AMutableOrderedList;
import edu.uci.ics.asterix.om.base.AMutablePoint;
import edu.uci.ics.asterix.om.base.AMutablePoint3D;
import edu.uci.ics.asterix.om.base.AMutablePolygon;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableRectangle;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.AMutableUnorderedList;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectFactory;

public class AMutableObjectFactory implements IObjectFactory<IAObject, IAType> {

    public static final AMutableObjectFactory INSTANCE = new AMutableObjectFactory();

    private AMutableObjectFactory() {
    }

    @Override
    public IAObject create(IAType type) {
        IAObject retValue = null;
        switch (type.getTypeTag()) {
            case INT32:
                retValue = new AMutableInt32(0);
                break;
            case STRING:
                retValue = new AMutableString("");
                break;
            case FLOAT:
                retValue = new AMutableFloat(0);
                break;
            case DOUBLE:
                retValue = new AMutableDouble(0);
                break;
            case CIRCLE:
                retValue = new AMutableCircle(new AMutablePoint(0, 0), 0);
                break;
            case POINT:
                retValue = new AMutablePoint(0, 0);
                break;
            case POINT3D:
                retValue = new AMutablePoint3D(0, 0, 0);
                break;
            case POLYGON:
                retValue = new AMutablePolygon(new AMutablePoint[] {});
                break;
            case LINE:
                retValue = new AMutableLine(new AMutablePoint(0, 0), new AMutablePoint(0, 0));
                break;
            case RECTANGLE:
                retValue = new AMutableRectangle(new AMutablePoint(0, 0), new AMutablePoint(1, 1));
                break;
            case DATE:
                retValue = new AMutableDate(0);
                break;
            case DATETIME:
                retValue = new AMutableDateTime(0);
                break;
            case DURATION:
                retValue = new AMutableDuration(0, 0);
                break;
            case INTERVAL:
                retValue = new AMutableInterval(0, 0, (byte) 0);
                break;
            case TIME:
                retValue = new AMutableTime(0);
                break;
            case INT64:
                retValue = new AMutableInt64(0);
                break;
            case ORDEREDLIST:
                AOrderedListType ot = (AOrderedListType) type;
                retValue = new AMutableOrderedList(ot);
                break;
            case UNORDEREDLIST:
                AUnorderedListType ut = (AUnorderedListType) type;
                retValue = new AMutableUnorderedList(ut);
                break;
            case RECORD:
                IAType[] fieldTypes = ((ARecordType) type).getFieldTypes();
                IAObject[] fieldObjects = new IAObject[fieldTypes.length];
                int index = 0;
                for (IAType fieldType : fieldTypes) {
                    fieldObjects[index] = create(fieldType);
                    index++;
                }
                retValue = new AMutableRecord((ARecordType) type, fieldObjects);
                break;
            case UNION:
                AUnionType unionType = (AUnionType) type;
                List<IAType> unionList = unionType.getUnionList();
                IAObject itemObject = null;
                for (IAType elementType : unionList) {
                    if (!elementType.getTypeTag().equals(ATypeTag.NULL)) {
                        itemObject = create(elementType);
                        break;
                    }
                }
                return retValue = itemObject;
        }
        return retValue;
    }
}
