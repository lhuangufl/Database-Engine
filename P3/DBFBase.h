#ifndef DBFBASE_H
#define DBFBASE_H

#include "BigQ.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

enum fType {heap, sorted, tree};
enum fMode {reading, writing};

const string fTypeStr[] = {string("heap"), string("sorted"), string("tree")};
const int BigQBuffSize = 1000;

class DBFBase {

public:
	File* file;

	Page currentPage;
	off_t currentDataPageIdx;
	fType myType;

	DBFBase (); 
	~DBFBase();

	off_t lastDataPageIdx() {
		off_t cur_len = file->GetLength();
    	off_t last_data_page_idx = cur_len - 2;
		return last_data_page_idx;
	}

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	
};
#endif
