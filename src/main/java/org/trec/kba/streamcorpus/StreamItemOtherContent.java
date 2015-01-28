package org.trec.kba.streamcorpus;

import java.util.Map;



public class StreamItemOtherContent extends StreamItem {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5244448927328731901L;
	
	Map<String,ContentItem> oc;
	public StreamItemOtherContent() {
		super();
		
		oc = this.getOther_content();
	}
	
	
	
	public ContentItem getTitle(){
		return oc.get("title");
	}
	
	public ContentItem getAnchor() {
		return oc.get("ancor");
	}

}
