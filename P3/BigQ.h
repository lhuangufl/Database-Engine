#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {
private:

	OrderMaker* order;
	int runlen;
	ComparisonEngine* comp;

	File file;

	vector<off_t> runIndexes;

public:
	Pipe* input, * output;
	pthread_t* thread = nullptr;
	BigQ(Pipe* in, Pipe* out, OrderMaker& sortorder, int runlen);
	~BigQ();

	int ExcuteSortPhase();
	int ExcuteMergePhase();
};

//Class run represent run used for merging
class Run {
private:
	File* file;
	Page bufferPage;
	off_t startPageIdx;
	off_t curPageIdx;
	int runlen;
public:
	Record* nextRecordPtr;
	Run(File* file, off_t startPageIndx, int runlen);
	//Used to update the top record of current run
	int NextRecord();
	Record* GetNextRecordPtr();
};

void* Worker(void* (BigQ::*));


#endif
