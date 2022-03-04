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
	Pipe* input, * output;
	OrderMaker* order;
	int runlen;
	ComparisonEngine* comp;

public:
	BigQ(Pipe& in, Pipe& out, OrderMaker& sortorder, int runlen);
	~BigQ();

	int ExcuteSortPhase();
};

//Class run represent run used for merging
class Run {
private:
	File* fileBase;
	Page bufferPage;
	int startPageIdx;
	int curPageIdx;
	int runlen;
public:
	Run(File* file, int startPageIndx, int runLength);
	//Used to update the top record of current run
	int UpdateTopRecord();
	//top record of current Run
	Record* topRecord;
};

//Class used for comparing records for given CNF


//Main method executed by worker, worker will retrieve records from input pipe,
//sort records into runs and puting all runs into priority queue, and geting sorted reecords
//from priority queue to output pipe
void* Worker(void* (BigQ::*));


#endif
