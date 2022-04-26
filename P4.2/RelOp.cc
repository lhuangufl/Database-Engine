#include "RelOp.h"

typedef struct {
	DBFile* inputFile;
	Pipe* outputPipe;
	CNF* selOp;
	Record* literal;
} SelectFileArgs;

void* SelectFileWorker(void* arg) {
	auto* args = (SelectFileArgs*)arg;
	Record tmpRecord;
	while (args->inputFile->GetNext(tmpRecord, *(args->selOp), *(args->literal))) {
		args->outputPipe->Insert(&tmpRecord);
	}
	args->outputPipe->ShutDown();
	return NULL;
}

void SelectFile::Run(DBFile& inFile, Pipe& outPipe, CNF& selOp, Record& literal) {
	auto* args = new SelectFileArgs{ &inFile, &outPipe, &selOp, &literal };
	pthread_create(&worker, NULL, SelectFileWorker, (void*)args);
}

typedef struct {
	Pipe* inputPipe;
	Pipe* outputPipe;
	CNF* selOp;
	Record* literal;
} SelectPipeArgs;

void* SelectPipeWorker(void* arg) {
	auto* args = (SelectPipeArgs*)arg;
	ComparisonEngine comparisonEngine;
	Record tmpRecord;
	while (args->inputPipe->Remove(&tmpRecord)) {
		if (comparisonEngine.Compare(&tmpRecord, (args->literal), (args->selOp))) {
			args->outputPipe->Insert(&tmpRecord);
		}
	}
	args->outputPipe->ShutDown();
	return NULL;
}

void SelectPipe::Run(Pipe& inPipe, Pipe& outPipe, CNF& selOp, Record& literal) {
	auto* args = new SelectPipeArgs{ &inPipe, &outPipe, &selOp, &literal };
	pthread_create(&worker, NULL, SelectPipeWorker, (void*)args);
}

typedef struct {
	Pipe* inputPipe;
	Pipe* outputPipe;
	int* keepMe;
	int numAttsInput, numAttsOutput;
} ProjectArgs;

void* ProjectWorker(void* arg) {
	auto* args = (ProjectArgs*)arg;
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
	auto* args = new ProjectArgs{ &inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput };
	pthread_create(&worker, NULL, ProjectWorker, (void*)args);
}

typedef struct {
	Pipe* inputPipeL;
	Pipe* inputPipeR;
	Pipe* outputPipe;
	CNF* selOp;
	Record* literal;
	int runLen;
} JoinArgs;


// This method is used to merge two records into single record
void JoinWorker_AddMergedRecord(Record* leftRecord, Record* rightRecord, Pipe* pipe) {
	int numOfAttsLeft = ((((int*)leftRecord->bits) [1]) / sizeof(int)) - 1;
	int numOfAttsRight = ((((int*)rightRecord->bits) [1]) / sizeof(int)) - 1;
	//std::cout << "numOfAttsLeft = " << numOfAttsLeft << " numOfAttsRight = " << numOfAttsRight << std::endl;
	int* attsToKeep = new int [numOfAttsLeft + numOfAttsRight];
	for (int i = 0; i < numOfAttsLeft; i++) {
		attsToKeep [i] = i;
	}
	for (int i = numOfAttsLeft; i < numOfAttsLeft + numOfAttsRight; i++) {
		attsToKeep [i] = i - numOfAttsLeft;
	}

	Record mergedRecord;
	mergedRecord.MergeRecords(leftRecord, rightRecord, numOfAttsLeft, numOfAttsRight, attsToKeep, numOfAttsLeft + numOfAttsRight, numOfAttsLeft);
	pipe->Insert(&mergedRecord);
}

//Sort merge Join
void JoinWorker_Merge(JoinArgs* joinArg, OrderMaker* leftOrder, OrderMaker* rightOrder) {

	Pipe* sortedLeftPipe = new Pipe(10000);
	Pipe* sortedRightPipe = new Pipe(10000);

	BigQ* bigQLeft = new BigQ(*(joinArg->inputPipeL), *sortedLeftPipe, *leftOrder, joinArg->runLen);
	sleep(1);
	BigQ* bigQRight = new BigQ(*(joinArg->inputPipeR), *sortedRightPipe, *rightOrder, joinArg->runLen);

	Record leftRecord, rightRecord;
	Record* prevLeftRecord; // to keep track of the previous left record removed from Sort Pipe
	bool doWeGetLeft = false, doWeGetRight = false;

	doWeGetLeft = sortedLeftPipe->Remove(&leftRecord);
	doWeGetRight = sortedRightPipe->Remove(&rightRecord);

	ComparisonEngine comparisonEngine;
	vector<Record*> buff;
	//Then do the merge part, merge same record together to JOIN
	//We must have a left record and right record, otherwise no JOIN
	while (doWeGetLeft && doWeGetRight) {
		int left_vs_right = comparisonEngine.Compare(&leftRecord, leftOrder, &rightRecord, rightOrder);
		// If left and right records are not equal, we gotta move on either right or left, depending on the order
		if (left_vs_right < 0) {
			doWeGetLeft = sortedLeftPipe->Remove(&leftRecord);
		}
		else if (left_vs_right > 0) {
			doWeGetRight = sortedRightPipe->Remove(&rightRecord);
		}
		//If left record equals to right record, we merge them together and insert it into output pipe
		else {
			// We gotta have a left record
			// We push back a series of consecutive and equal records into buffer container for later JOIN
			while (doWeGetLeft && (prevLeftRecord == NULL ||
				comparisonEngine.Compare(prevLeftRecord, &leftRecord, leftOrder) == 0))
			{
				prevLeftRecord = new Record();
				prevLeftRecord->Consume(&leftRecord);
				buff.push_back(prevLeftRecord);
				doWeGetLeft = sortedLeftPipe->Remove(&leftRecord);
			}

			// We gotta have a right record, AND, left Record equals to Right Record
			while (doWeGetRight &&
				comparisonEngine.Compare(prevLeftRecord, leftOrder, &rightRecord, rightOrder) == 0)
			{
				for (int i = 0; i < buff.size(); i++) {
					JoinWorker_AddMergedRecord(buff [i], &rightRecord, joinArg->outputPipe);
				}
				// Find the nex right record, And then check if this one also equals to the left record
				doWeGetRight = sortedRightPipe->Remove(&rightRecord);
			}
			buff.clear();
			prevLeftRecord = NULL;
		}
	}

	while (sortedLeftPipe->Remove(&leftRecord) == 1);
	while (sortedRightPipe->Remove(&rightRecord) == 1);
	delete sortedLeftPipe; delete sortedRightPipe;
	delete bigQLeft; delete bigQRight;
	delete prevLeftRecord;
}

void* JoinWorker(void* arg) {
	auto* args = (JoinArgs*)arg;
	OrderMaker leftOrder, rightOrder;
	args->selOp->GetSortOrders(leftOrder, rightOrder);
	JoinWorker_Merge(args, &leftOrder, &rightOrder);
	args->outputPipe->ShutDown();
	return NULL;
}


void Join::Run(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, CNF& selOp, Record& literal) {
	auto* args = new JoinArgs{ &inPipeL, &inPipeR, &outPipe, &selOp, &literal, this->runlen };
	pthread_create(&worker, NULL, JoinWorker, (void*)args);
}


typedef struct {
	Pipe* inputPipe;
	Pipe* outputPipe;
	Schema* schema;
	int runLen;
} DuplicateRemovalArgs;

void* DuplicateRemovalWorker(void* arg) {
	auto* args = (DuplicateRemovalArgs*)arg;
	ComparisonEngine comparisonEngine;
	Record cur, * prev;
	Pipe* sortedPipe = new Pipe(10000);
	OrderMaker* orderMaker = new OrderMaker(args->schema);
	BigQ* bigQ = new BigQ(*(args->inputPipe), *sortedPipe, *orderMaker, args->runLen);

	while (sortedPipe->Remove(&cur)) {
		if (prev == NULL || comparisonEngine.Compare(&cur, prev, orderMaker) != 0) {
			if (prev == NULL) prev = new Record();
			prev->Copy(&cur);
			args->outputPipe->Insert(&cur);
		}
	}
	args->outputPipe->ShutDown();
	delete prev; delete sortedPipe; delete bigQ;
}

void DuplicateRemoval::Run(Pipe& inPipe, Pipe& outPipe, Schema& mySchema) {
	auto* args = new DuplicateRemovalArgs{ &inPipe, &outPipe, &mySchema, this->runlen };
	pthread_create(&worker, NULL, DuplicateRemovalWorker, (void*)args);
}

typedef struct {
	Pipe* inputPipe;
	Pipe* outputPipe;
	Function* computeMe;
} SumArgs;

void* SumWorker(void* arg) {
	auto sum = 0, intResult = 0;
	auto doubleSum = 0.0, doubleResult = 0.0;
	auto* args = (SumArgs*)arg;
	Type type;
	ComparisonEngine comparisonEngine;
	Record tmpRecord;
	while (args->inputPipe->Remove(&tmpRecord)) {
		type = args->computeMe->Apply(tmpRecord, intResult, doubleResult);
		if (type == Int)
			sum += intResult;
		else if (type == Double)
			doubleSum += doubleResult;
		else throw std::runtime_error("Data type for sum is not supported");
	}
	// Next compose a Record for Sum result and dump it into outPipe
	Attribute  attribute = { "SUM", type };
	Schema schema("schema", 1, &attribute);
	char recordCharacter [100];
	if (type == Int) sprintf(recordCharacter, "%d|", sum);
	else if (type == Double) sprintf(recordCharacter, "%f|", doubleSum);
	else throw std::runtime_error("Data type for sum is not supported");
	tmpRecord.ComposeRecord(&schema, recordCharacter);
	args->outputPipe->Insert(&tmpRecord);
	args->outputPipe->ShutDown();
	return NULL;
}

void Sum::Run(Pipe& inPipe, Pipe& outPipe, Function& computeMe) {
	auto* args = new SumArgs{ &inPipe, &outPipe, &computeMe };
	pthread_create(&worker, NULL, SumWorker, (void*)args);
}

typedef struct {
	Pipe* inPipe;
	Pipe* outPipe;
	OrderMaker* groupAtts;
	Function* computeMe;
	int runLen;
} GroupByArgs;

void* GroupByWorker(void* arg) {

	auto* args = (GroupByArgs*)arg;

	Record cur, * prev = NULL;
	int int_res = 0, int_sum = 0;
	double double_res = 0, double_sum = 0;
	Type type;
	int cnt = 0;

	int runLength = 100;
	int buffsz = 200;

	Pipe* bigq_out = new Pipe(buffsz);

	sleep(3);
	BigQ bigq(*(args->inPipe), *bigq_out, *(args->groupAtts), runLength);

	ComparisonEngine comparisonEngine;

	bool doWeHaveMoreRecords = false;
	int haveReadCur = 0;

	while (doWeHaveMoreRecords || (haveReadCur = bigq_out->Remove(&cur)) || prev != NULL) {
		if ((doWeHaveMoreRecords || haveReadCur) && (prev == NULL || comparisonEngine.Compare(prev, &cur, args->groupAtts) == 0)) {
			if (prev == NULL) {
				prev = new Record();
				prev->Copy(&cur);
				doWeHaveMoreRecords = false;
			}
			type = args->computeMe->Apply(cur, int_res, double_res);
			switch (type) {
				case Int:
					int_sum += int_res;
					int_res = 0;
					break;
				case Double:
					double_sum += double_res;
					double_res = 0;
					break;
				default:
				std:cerr << "[Error] In RunFunc::Sum(): invalid sum result type" << std::endl;
			}
		}
		else {
			Record group_sum;

			Attribute sum_att = { "sum", type };
			Attribute sum_atts [1] = { sum_att };
			Attribute group_atts [MAX_NUM_ATTS];

			Attribute out_atts [MAX_NUM_ATTS + 1];
			int group_att_indices [MAX_NUM_ATTS + 1];
			int numAtts = args->groupAtts->GetAtts(group_atts, group_att_indices);
			int numOutAtts = Util::catArrays<Attribute>(out_atts, sum_atts, group_atts, 1, numAtts);

			Schema group_sch("group_sch", numOutAtts, out_atts);
			const char* group_str;
			std::string tmp;
			if (type == Int) {
				tmp = Util::toString<int>(int_sum) + "|";
			}
			else {
				tmp = Util::toString<double>(double_sum) + "|";
			}
			for (int i = 0; i < numAtts; i++) {
				switch (group_atts [i].myType) {
					case Int: {
							int att_val_int = *(prev->GetAttVal<int>(group_att_indices [i]));
							tmp += Util::toString<int>(att_val_int) + "|";
							break;
						}
					case Double: {
							double att_val_double = *(prev->GetAttVal<double>(group_att_indices [i]));
							tmp += Util::toString<double>(att_val_double) + "|";
							break;
						}
					case String: {
							std::string att_val_str(prev->GetAttVal<char>(group_att_indices [i]));
							tmp += att_val_str + "|";
							break;
						}
				}
			}
			group_str = tmp.c_str();
			group_sum.ComposeRecord(&group_sch, group_str);

			args->outPipe->Insert(&group_sum);

			delete prev;
			prev = NULL;
			if (haveReadCur) {
				doWeHaveMoreRecords = true;
			}
		}
	}

	args->outPipe->ShutDown();
}

void GroupBy::Run(Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe) {
	auto* args = new GroupByArgs{ &inPipe, &outPipe, &groupAtts, &computeMe, this->runlen };
	pthread_create(&worker, NULL, GroupByWorker, (void*)args);
}

typedef struct {
	Pipe* inPipe;
	FILE* outFile;
	Schema* schema;
} WriteOutArgs;

void* WriteOutWorker(void* arg) {
	auto* writeOutArg = (WriteOutArgs*)arg;
	Record cur;
	while (writeOutArg->inPipe->Remove(&cur) == 1) {
		int numOfAtts = writeOutArg->schema->GetNumAtts();
		Attribute* attribute = writeOutArg->schema->GetAtts();
		for (int i = 0; i < numOfAtts; i++) {
			fprintf(writeOutArg->outFile, "%s:", attribute [i].name);
			int pointer = ((int*)cur.bits) [i + 1];
			fprintf(writeOutArg->outFile, "[");
			if (attribute [i].myType == Int) {
				int* writeOutInt = (int*)&(cur.bits [pointer]);
				fprintf(writeOutArg->outFile, "%d", *writeOutInt);
			}
			else if (attribute [i].myType == Double) {
				double* writeOutDouble = (double*)&(cur.bits [pointer]);
				fprintf(writeOutArg->outFile, "%f", *writeOutDouble);
			}
			else if (attribute [i].myType == String) {
				char* writeOutString = (char*)&(cur.bits [pointer]);
				fprintf(writeOutArg->outFile, "%s", writeOutString);
			}
			fprintf(writeOutArg->outFile, "]");
			if (i != numOfAtts - 1)
				fprintf(writeOutArg->outFile, ", ");
		}
		fprintf(writeOutArg->outFile, "\n");
	}
	return NULL;
}

void WriteOut::Run(Pipe& inPipe, FILE* outFile, Schema& mySchema) {
	auto* args = new WriteOutArgs{ &inPipe, outFile, &mySchema };
	pthread_create(&worker, NULL, WriteOutWorker, (void*)args);
}
