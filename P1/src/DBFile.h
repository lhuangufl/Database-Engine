#ifndef DBFILE_H
#define DBFILE_H

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

typedef enum { heap, sorted, tree } fType;

// stub DBFile header..replace it with your own DBFile.h

class DBFile {
	  char file_path[20];
	  Record *currentRecord;
	  Page *readPage;
	  Page *writePage;
	  File *file;
	  off_t pageIndex;
	  off_t writeIndex;
	  char *name;
	  bool writeIsDirty;
	  bool endOfFile;

        public:
	  DBFile();
	  ~DBFile();

	  int Create(char *fpath, fType file_type, void *startup);
	  int Open(char *fpath);
	  int Close();

	  void Load(Schema &myschema, char *loadpath);

	  void MoveFirst();
	  void Add(Record &addme);
	  int GetNext(Record &fetchme);
	  int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};
#endif
