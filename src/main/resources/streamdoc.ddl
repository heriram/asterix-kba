create dataverse feeds if not exists;
use dataverse feeds;
drop dataset StreamDocuments if exists;
drop type StreamType if exists;
drop type InputRecordType if exists;
create type InputRecordType as open {
        doc_id: string,
        stream_id: string,
        title_cleansed: string,
        body_cleansed: [string],
        source: string,
        dir_name: string,
        anchor_cleansed: string,
        language: string,
        schost: string,
        mentions: {{string}},
		parent: string?
}

create type StreamType as open {
        doc_id: string,
        stream_id: string,
        title_cleansed: string,
        body_cleansed: [string],
        source: string,
        dir_name: string,
        anchor_cleansed: string,
        language: string,
        schost: string,
        mentions: {{string}}
}

create dataset StreamDocuments(StreamType)
primary key doc_id;

drop dataset ChildStreamDocuments if exists;
drop type ChildStreamType if exists;
create type ChildStreamType as open {
        doc_id: string,
		body_cleansed: [string],
        parent:string
}
create dataset ChildStreamDocuments(ChildStreamType)
primary key doc_id;

create feed kbafeed using "kbalib#push_kba_stream_feed"
(("type-name"="StreamType"), ("batch-size"="300"),
("pre-filter-mentions"="yes"),("path"="127.0.0.1:///Users/heri/git/corpus_test"));

create secondary feed kbachildfeed from feed kbafeed
	apply function "kbalib#childDocLoader";

connect feed kbafeed to dataset StreamDocuments;
connect feed kbachildfeed to dataset ChildStreamDocuments;

// QUERYING:
use dataverse feeds;
for $d in dataset StreamDocuments where count(for $i in $d.mentions return $i)>0 return {
"doc-id": $d.doc_id, 
"mentions": $d.mentions};
