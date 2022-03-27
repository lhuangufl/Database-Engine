

#ifndef DBFILESORTED_H
#define DBFILESORTED_H

#include "DBFile.h"
#include <queue>
#include "BigQ.h"


class DBFileSorted :public DBFileGeneric {
	friend class DBFile;
private:
	File dbDiskFile;
	Page bufferPage;
	off_t pageIndex;
	int isWriting;
	char* outputPath = nullptr;
	int boundCalculated = 0;
	int lowerBound;
	int higherBound;

	Pipe* in = nullptr;
	Pipe* out = nullptr;

	BigQ* bigQ = nullptr;

	OrderMaker* orderMaker = nullptr;
	int runlen;

	void writeMode();
	void readMode();
	static void* consumer(void* arg);
	int Run(Record* left, Record* literal, Comparison* c);
public:
	DBFileSorted();

	int Create(char* fpath, fType f_type, void* startup);
	int Open(char* fpath);
	int Close();

	void Load(Schema& myschema, char* loadpath);

	void MoveFirst();
	void Add(Record& addme);
	int GetNext(Record& fetchme);
	int GetNext(Record& fetchme, CNF& cnf, Record& literal);

};


#endif
