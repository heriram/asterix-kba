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
package edu.uci.ics.asterix.external.library.udf;

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
import edu.uci.ics.asterix.external.library.java.JTypeTag;

public class HashTagsFunction implements IExternalScalarFunction {

	private JUnorderedList list = null;

	@Override
	public void initialize(IFunctionHelper functionHelper) {
		list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
	}

	@Override
	public void deinitialize() {
	}

	@Override
	public void evaluate(IFunctionHelper functionHelper) throws Exception {
		list.clear();
		JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
		JString text = (JString) inputRecord.getValueByName("message_text");

		String[] tokens = text.getValue().split(" ");
		for (String tk : tokens) {
			if (tk.startsWith("#")) {
				JString newField = (JString) functionHelper
						.getObject(JTypeTag.STRING);
				newField.setValue(tk);
				list.add(newField);
			}
		}

		/*JRecord result = (JRecord) functionHelper.getResultObject();
		result.setField("tweetid", inputRecord.getFields()[0]);
		result.setField("user", inputRecord.getFields()[1]);
		result.setField("location_lat", inputRecord.getFields()[2]);
		result.setField("location_long", inputRecord.getFields()[3]);
		result.setField("send_time", inputRecord.getFields()[4]);
		result.setField("message_text", inputRecord.getFields()[5]);
		result.setField("topics", list);
		*/
		JRecord result = (JRecord) functionHelper.getResultObject();
		result.setField("id", inputRecord.getFields()[0]);
		result.setField("user_name", inputRecord.getFields()[1]);
		result.setField("latitude", inputRecord.getFields()[2]);
		result.setField("longitude", inputRecord.getFields()[3]);
		result.setField("created_at", inputRecord.getFields()[4]);
		result.setField("message_text", inputRecord.getFields()[5]);
		result.setField("country", inputRecord.getFields()[7]);
		result.setField("topics", list);
		functionHelper.setResult(result);
	}

}
