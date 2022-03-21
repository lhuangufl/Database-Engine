
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileSorted.h"
#include "Defs.h"
#include <chrono>
#include <thread>
#include <pthread.h>
#include <set>
#include <string.h>


DBFileSorted::DBFileSorted() {
	isWriting = 0;
	pageIndex = 0;
}

int DBFileSorted::Create(char* f_path, fType f_type, void* startup) {

	dbDiskFile.Open(0, f_path);
	outputPath = f_path;
	pageIndex = 0;
	isWriting = 0;
	MoveFirst();

	return 1;
}


void DBFileSorted::Load(Schema& f_schema, char* loadpath) {
	FILE* tableFile = fopen(loadpath, "r");
	Record temp;
	ComparisonEngine comp;

	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
		this->Add(temp);
	}
	//    in->ShutDown();
	fclose(tableFile);
}

int DBFileSorted::Open(char* f_path) {
	dbDiskFile.Open(1, f_path);
	pageIndex = 0;
	outputPath = f_path;
	//Reading mode on default
	isWriting = 0;
	MoveFirst();
	return 1;
}

void DBFileSorted::MoveFirst() {
	readMode();
	pageIndex = 0;
	bufferPage.EmptyItOut();
	//If DBfile is not empty
	if (dbDiskFile.GetLength() > 0) {
		dbDiskFile.GetPage(&bufferPage, pageIndex);
	}
}

int DBFileSorted::Close() {
	readMode();
	bufferPage.EmptyItOut();
	dbDiskFile.Close();
	return 1;
}

void DBFileSorted::Add(Record& rec) {

	writeMode();
	in->Insert(&rec);
}
void DBFileSorted::writeMode() {
	if (isWriting == 0) {
		isWriting = 1;
		in = new Pipe(128);
		out = new Pipe(128);
		// Invoke BigQ constructor which will sort the records from input pipe and
		// write the merged records into output pipe.
		bigQ = new BigQ(in, out, *orderMaker, runlen);
	}
}

void DBFileSorted::readMode() {
	if (isWriting == 1) {
		isWriting = 0;
		in->ShutDown();
		Record temp;
		DBFileHeap tempHeapFile;
		tempHeapFile.Create("tmp.heap", heap, NULL);

		while (out->Remove(&temp)) {

			tempHeapFile.Add(temp);
		}

		if (in != nullptr) delete in;
		if (out != nullptr) delete out;
		if (bigQ->thread != nullptr) delete bigQ->thread;
		tempHeapFile.Close();
		rename("tmp.heap", outputPath);
	}
}


int DBFileSorted::GetNext(Record& fetchme) {
	//If file is writing, then write page into disk based file, redirect page ot first page
	readMode();
	//If reach the end of page
    //    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

	if (bufferPage.GetFirst(&fetchme) == 0) {
		//        cout<<dbDiskFile.GetLength()<<endl;
		pageIndex++;
		//If reach the end of file
		if (pageIndex >= dbDiskFile.GetLength() - 1) {
			return 0;
		}
		//Else get next page
		bufferPage.EmptyItOut();
		dbDiskFile.GetPage(&bufferPage, pageIndex);
		bufferPage.GetFirst(&fetchme);
	}
	return 1;
}

int DBFileSorted::GetNext(Record& fetchme, CNF& cnf, Record& literal) {
	if (!boundCalculated) {
		boundCalculated = 1;
		set<int> attSet;
		for (int i = 0; i < orderMaker->numAtts; i++) {
			attSet.insert(orderMaker->whichAtts [i]);
		}
		int global_lower = 0, global_higher = dbDiskFile.GetLength() - 2;
		cout << "original lower bound: " << global_lower << endl;
		cout << "original higher bound: " << global_higher << endl;
		for (int i = 0; i < cnf.numAnds; i++) {
			for (int j = 0; j < cnf.orLens [i]; j++) {
				if (attSet.find(cnf.orList [i][j].whichAtt1) == attSet.end()) continue;
				//calculate the lower bound and upper bound by Binary Search
				// calculate the upper bound for a LessThan
				if (cnf.orList [i][j].op == LessThan) {
					int left = 0, right = dbDiskFile.GetLength() - 2;
					Record rec;
					while (left < right) {
						int mid = left + (right - left + 1) / 2;
						dbDiskFile.GetPage(&bufferPage, mid);
						bufferPage.GetFirst(&rec);
						int result = Run(&rec, &literal, &cnf.orList [i][j]);
						if (result != 0) {
							left = mid;
						}
						else {
							right = mid - 1;
						}
					}
					//update the global lower and upper bound
					global_higher = min(global_higher, right);
				}
				// calculate the lower bound for a UpperThan
				else if (cnf.orList [i][j].op == GreaterThan) {
					int left = 0, right = dbDiskFile.GetLength() - 2;
					Record rec;
					while (left < right) {
						int mid = left + (right - left) / 2;
						dbDiskFile.GetPage(&bufferPage, mid);
						bufferPage.GetFirst(&rec);
						int result = Run(&rec, &literal, &cnf.orList [i][j]);
						if (result != 0) {
							right = mid;
						}
						else {
							left = mid + 1;
						}
					}
					//update the global lower and upper bound
					global_lower = max(global_lower, left);
				}
			}
		}
		global_lower = global_lower - 1; // left shift for a page
		global_lower = max(0, global_lower);

		cout << "updated lower bound by Binary Search: " << global_lower << endl;
		cout << "updated higher bound by Binary Search: " << global_higher << endl;
		lowerBound = global_lower;
		higherBound = global_higher;
		pageIndex = global_lower;
	}

	ComparisonEngine comp;
	//If not reach the end of file

	while (GetNext(fetchme) == 1) {
		if (pageIndex > higherBound) return 0;
		if (comp.Compare(&fetchme, &literal, &cnf) == 1)
			return 1;
	}
	return 0;
}




int DBFileSorted::Run(Record* left, Record* literal, Comparison* c) {

	char* val1, * val2;

	char* left_bits = left->GetBits();
	char* lit_bits = literal->GetBits();

	// first get a pointer to the first value to compare
	if (c->operand1 == Left) {
		val1 = left_bits + ((int*)left_bits) [c->whichAtt1 + 1];
	}
	else {
		val1 = lit_bits + ((int*)lit_bits) [c->whichAtt1 + 1];
	}

	// next get a pointer to the second value to compare
	if (c->operand2 == Left) {
		val2 = left_bits + ((int*)left_bits) [c->whichAtt2 + 1];
	}
	else {
		val2 = lit_bits + ((int*)lit_bits) [c->whichAtt2 + 1];
	}


	int val1Int, val2Int, tempResult;
	double val1Double, val2Double;

	// now check the type and the comparison operation
	switch (c->attType) {

		// first case: we are dealing with integers
		case Int:

			val1Int = *((int*)val1);
			val2Int = *((int*)val2);

			// and check the operation type in order to actually do the comparison
			switch (c->op) {

				case LessThan:
					return (val1Int < val2Int);
					break;

				case GreaterThan:
					return (val1Int > val2Int);
					break;

				default:
					return (val1Int == val2Int);
					break;
			}
			break;

			// second case: dealing with doubles
		case Double:
			val1Double = *((double*)val1);
			val2Double = *((double*)val2);

			// and check the operation type in order to actually do the comparison
			switch (c->op) {

				case LessThan:
					return (val1Double < val2Double);
					break;

				case GreaterThan:
					return (val1Double > val2Double);
					break;

				default:
					return (val1Double == val2Double);
					break;
			}
			break;

			// final case: dealing with strings
		default:

			// so check the operation type in order to actually do the comparison
			tempResult = strcmp(val1, val2);
			switch (c->op) {

				case LessThan:
					return tempResult < 0;
					break;

				case GreaterThan:
					return tempResult > 0;
					break;

				default:
					return tempResult == 0;
					break;
			}
			break;
	}

}
