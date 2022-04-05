#include "RelOp.h"

typedef struct {
	DBFile& inputFile;
	Pipe& outputPipe;
	CNF& selOp;
	Record& literal;
} SelectFileArgs;

void* SelectFileWorker(void* arg) {
	SelectFileArgs* args = (SelectFileArgs*)arg;
	Record tmpRecord;
	while (args->inputFile.GetNext(tmpRecord, SelectFileArgs->selOp, SelectFileArgs->literal)) {
		SelectFileArgs->outputPipe->Insert(&tmpRecord);
	}
	SelectFileArgs->outputPipe->ShutDown();
	return NULL;
}

void SelectFile::Run(DBFile& inFile, Pipe& outPipe, CNF& selOp, Record& literal) {
	SelectFileArgs* args = new SelectFileArgs{ inFile, outPipe, selOp, literal };
	pthread_create(&thread, NULL, SelectFileWorker, (void*)SelectFileArgs);
}

void SelectFile::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {

}

typedef struct {
	Pipe& inputPipe;
	Pipe& outputPipe;
	CNF& selOp;
	Record& literal;
} SelectPipeArgs;

void* SelectPipeWorker(void* arg) {
	auto args = (SelectPipeArgs*)arg;
	ComparisonEngine comp;
	Record tmpRecord;
	while (args->inputPipe.Remove(&tmpRecord)) {
		if (comp.Compare(&tmpRecord, &args->literal, &args->selOp)) {
			args->outputPipe.Insert(&tmpRecord);
		}
	}
	args->outputPipe.ShutDown();
	return NULL;
}

void SelectPipe::Run(Pipe& inPipe, Pipe& outPipe, CNF& selOp, Record& literal) {
	auto* args = new SelectPipeArgs{ inPipe, outPipe, selOp, literal };
	pthread_create(&thread, NULL, SelectPipeWorker, (void*)SelectPipeArgs);
}

void SelectPipe::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {

}

typedef struct {
	Pipe& inputPipe;
	Pipe& outputPipe;
	int* keepMe;
	int numAttsInput, numAttsOutput;
} ProjectArgs;

void* ProjectWorker(void* arg) {
	auto args = (ProjectArgs*)arg;
	Record tmpRecord;
	while (args->inputPipe->Remove(&tmpRecord)) {
		Record* recordPtrToOutpipe = new Record;
		recordPtrToOutpipe->Consume(&tmpRecord);
		recordPtrToOutpipe->Project(args->keepMe, args->numAttsOutput, args->numAttsInput);
		args->outputPipe->Insert(recordPtrToOutpipe);
	}
	args->outputPipe->ShutDown();
	return NULL;
}


void Project::Run(Pipe& inPipe, Pipe& outPipe, int* keepMe, int numAttsInput, int numAttsOutput) {
	auto* args = new ProjectArgs{ inPipe, outPipe, keepMe, numAttsInput, numAttsOutput };
	pthread_create(&thread, NULL, ProjectWorker, (void*)args);
}

void Project::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int runlen) {

}

typedef struct {
	Pipe& inputPipeL;
	Pipe& inputPipeR;
	Pipe& outputPipe;
	CNF& selOp;
	Record& literal;

} JoinArgs;

void* JoinWorker(void* arg) {
	auto* joinArg = (JoinArgs*)arg;
	OrderMaker leftOrder, rightOrder;
	joinArg->selOp->GetSortOrders(leftOrder, rightOrder);
	//Decide to sort join merge or blocknested join
	if (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) {
		cout << "Enter sort merge " << endl;
		JoinWorker_Merge(joinArg, &leftOrder, &rightOrder);
	}
	else {
		cout << "BlockNestJoin" << endl;
		JoinWorker_BlockNested(joinArg);
	}
	joinArg->outPipe->ShutDown();
	return NULL;

}

//This method is used to merge two records into single record
void JoinWorker_AddMergedRecord(Record* leftRecord, Record* rightRecord, Pipe* pipe) {
	int numOfAttsLeft = ((((int*)leftRecord->bits) [1]) / sizeof(int)) - 1;
	int numOfAttsRight = ((((int*)rightRecord->bits) [1]) / sizeof(int)) - 1;
	int* attsToKeep = new int [numOfAttsLeft + numOfAttsRight];
	for (int i = 0; i < numOfAttsLeft; i++)
		attsToKeep [i] = i;
	for (int i = numOfAttsLeft; i < numOfAttsLeft + numOfAttsRight; i++)
		attsToKeep [i] = i - numOfAttsLeft;
	Record mergedRecord;
	mergedRecord.MergeRecords(leftRecord, rightRecord, numOfAttsLeft, numOfAttsRight, attsToKeep, numOfAttsLeft + numOfAttsRight, numOfAttsLeft);
	pipe->Insert(&mergedRecord);
}

//Sort merge Join
void JoinWorker_Merge(JoinArg* joinArg, OrderMaker* leftOrder, OrderMaker* rightOrder) {
	//First using BigQ to sort given records in two pipes
	Pipe* sortedLeftPipe = new Pipe(1000);
	Pipe* sortedRightPipe = new Pipe(1000);
	BigQ* tempL = new BigQ(*(joinArg->inPipeL), *sortedLeftPipe, *leftOrder, joinArg->runLen);
	BigQ* tempR = new BigQ(*(joinArg->inPipeR), *sortedRightPipe, *rightOrder, joinArg->runLen);
	// cout << "BigQ created" << endl;
	Record leftRecord;
	Record rightRecord;
	bool isFinish = false;
	if (sortedLeftPipe->Remove(&leftRecord) == 0)
		isFinish = true;
	if (sortedRightPipe->Remove(&rightRecord) == 0)
		isFinish = true;
	// cout << "BigQ outputed" << endl;
	ComparisonEngine comparisonEngine;
	//Then do the merge part, merge same record together to join
	while (!isFinish) {
		int compareRes = comparisonEngine.Compare(&leftRecord, leftOrder, &rightRecord, rightOrder);
		//If left record equal to right record, we merge them together and insert it into output pipe
		if (compareRes == 0) {
			vector<Record*> vl;
			vector<Record*> vr;
			//Find all idential and continuous records in the left pipe and put them into vector
			while (true) {
				Record* oldLeftRecord = new Record;
				oldLeftRecord->Consume(&leftRecord);
				vl.push_back(oldLeftRecord);
				if (sortedLeftPipe->Remove(&leftRecord) == 0) {
					isFinish = true;
					break;
				}
				if (comparisonEngine.Compare(&leftRecord, oldLeftRecord, leftOrder) != 0) {
					break;
				}
			}
			//Find all idential and continuous records in the right pipe and put them into vector
			while (true) {
				Record* oldRightRecord = new Record;
				oldRightRecord->Consume(&rightRecord);
				// oldRightRecord->Print(&partsupp);
				vr.push_back(oldRightRecord);
				if (sortedRightPipe->Remove(&rightRecord) == 0) {
					isFinish = true;
					break;
				}
				if (comparisonEngine.Compare(&rightRecord, oldRightRecord, rightOrder) != 0) {
					break;
				}
			}
			//Merge every part of them, and join
			for (int i = 0; i < vl.size(); i++) {
				for (int j = 0; j < vr.size(); j++) {
					JoinWorker_AddMergedRecord(vl [i], vr [j], joinArg->outPipe);
				}
			}
			vl.clear();
			vr.clear();
		}
		//If left reocrd are larger, then we move right record
		else if (compareRes > 0) {
			if (sortedRightPipe->Remove(&rightRecord) == 0)
				isFinish = true;
		}
		//If right record are larger, then we move left record
		else {
			if (sortedLeftPipe->Remove(&leftRecord) == 0)
				isFinish = true;
		}
	}
	cout << "Finish read fron sorted pipe" << endl;
	while (sortedLeftPipe->Remove(&leftRecord) == 1);
	while (sortedRightPipe->Remove(&rightRecord) == 1);
}

//This is block nested join on default
void JoinWorker_BlockNested(JoinArg* joinArg) {
	//Literlly join every pair of records.
	DBFile tempFile;
	char* fileName = new char [100];
	sprintf(fileName, "BlockNestedTemp%d.bin", pthread_self());
	tempFile.Create(fileName, heap, NULL);
	tempFile.Open(fileName);
	Record record;
	while (joinArg->inPipeL->Remove(&record) == 1)
		tempFile.Add(record);

	Record record1, record2;
	ComparisonEngine comparisonEngine;
	while (joinArg->inPipeR->Remove(&record1) == 1) {
		tempFile.MoveFirst();
		while (tempFile.GetNext(record) == 1) {
			if (comparisonEngine.Compare(&record1, &record2, joinArg->literal, joinArg->selOp)) {
				JoinWorker_AddMergedRecord(&record1, &record2, joinArg->outPipe);
			}
		}
	}
}

void Join::Run(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, CNF& selOp, Record& literal) {
	auto* args = new JoinArgs{ inPipeL, inPipeR, outPipe, selOp, literal };
	pthread_create(&thread, NULL, JoinWorker, (void*)args);
}

void Join::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int runlen) {
	this->runlen = runlen;
}

typedef struct {
	Pipe& inputPipe;
	Pipe& outputPipe;
	Schema& schema;
} DuplicateRemovalArgs;

void* DuplicateRemovalWorker(void* arg) {
	auto* args = (DuplicateRemovalArgs*)arg;
	ComparisonEngine comp;
	Record* cur, * prev;
	Pipe* sortedPipe = new Pipe(1000);
	OrderMaker* orderMaker = new OrderMaker(args->schema);
	BigQ* bigQ = new BigQ(*(args->inputPipe), *sortedPipe, orderMaker, 8);
	sortedPipe->Remove(prev);
	while (sortedPipe->Remove(cur)) {
		if (comp.Compare(cur, prev, orderMaker) != 0) {
			prev = cur;
			outputPipe->Insert(cur);
		}
	}
	args->outputPipe->ShutDown();
}

void DuplicateRemoval::Run(Pipe& inPipe, Pipe& outPipe, Schema& mySchema) {
	auto* args = new DuplicateRemovalArgs{ inPipe, outPipe, mySchema };
	pthread_create(&thread, NULL, DuplicateRemovalWorker, (void*)args);
}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
this->runlen = runlen;
}

typedef struct {
	Pipe& inputPipe;
	Pipe& outputPipe;
	Function& computeMe;
} SumArgs;

void* SumWorker(void* arg) {
	auto sum = 0, intResult, doubleResult;
	auto* args = (SumArgs*)arg;
	Type type;
	ComparisonEngine comp;
	Record tmpRecord;
	while (args->inputPipe.Remove(&tmpRecord)) {
		type = args->computeMe->Apply(tmpRecord, intResult, doubleResult);
		if (type == Int)
			sum += intResult;
		else if (type == Double)
			sum += doubleResult;
		else throw std::runtime_error("Data type for sum is not supported");
	}
	Attribute  attribute = { "SUM", type };

}

void Sum::Run(Pipe& inPipe, Pipe& outPipe, Function& computeMe) {
	auto* args = new SumArgs(inPipe, outPipe, computeMe);
	pthread_create(&thread, NULL, SumWorker, (void*)args);
}

void Sum::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int runlen) {

}

void GroupBy::Run(Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe) {

}

void GroupBy::WaitUntilDone() {

}

void GroupBy::Use_n_Pages(int runlen) {

}

void WriteOut::Run(Pipe& inPipe, FILE* outFile, Schema& mySchema) {

}

void WriteOut::WaitUntilDone() {

}

void WriteOut::Use_n_Pages(int runlen) {

}
