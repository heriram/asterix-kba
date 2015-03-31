use dataverse feeds;
drop dataset DocumentFeatures if exists;
drop dataset DocumentEntityFeatures if exists;
drop type DocumentEntityFeatureType if exists;
drop type DocumentFeatureType if exists;
create type DocumentEntityFeatureType as open {
        doc_id: string,
	    MentionsTitle: int32,
	    MentionsBody: int32,
	    MentionsAnchor: int32,
	    FirstPos: int32,
	    LastPos: int32,
	    Spread: int32,
	    FirstPosNorm: double,
	    LastPosNorm: double,
	    SpreadNorm: double
}

create type DocumentFeatureType as open {
        doc_id: string,
	    LengthTitle: int32,
	    LengthBody: int32,
	    LengthAnchor: int32,
	    Source: int32,
	    English: int32
}

create type RelatedEntityFeatureType as open {
        doc_id: string,
        Related: int32,
	    RelatedTitle: int32,
	    RelatedBody: int32,
	    RelatedAnchor: int32
}

create dataset RelatedEntityFeatures(RelatedEntityFeatureType)
primary key doc_id;

create dataset DocumentFeatures(DocumentFeatureType)
primary key doc_id;

create dataset DocumentEntityFeatures(DocumentEntityFeatureType)
primary key doc_id;

create secondary feed docfeat from feed kbafeed
	apply function "kbalib#documentFeature";

create secondary feed docentityfeat from feed kbafeed
		apply function "kbalib#documentEntityFeature";

create secondary feed relatedfeat from feed kbafeed
	apply function "kbalib#relatedEntityFeature";



