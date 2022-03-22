#include "BigQ.h"
#include "DBFileHeap.h"

void* Worker(void* bigQ) {

	auto* bigq = (BigQ*)bigQ;

	bigq->ExcuteSortPhase();
	bigq->ExcuteMergePhase();

	return nullptr;

	//Set disk based file for sorting
}

int BigQ::ExcuteSortPhase() {
	//Buffer page used for disk based file
	Page bufferPage;
	Record curRecord;
	off_t pageIndex = 0;
	int pageCounter = 0;
	int currentSize = 0;
	// Custom comparator that defines the order for our priority queue.
	auto RecordComparator = [this](Record* left, Record* right) {
		return (comp->Compare(left, right, order) >= 0);
	};

	//Retrieve all records from input pipe
	priority_queue<Record*, vector<Record*>, decltype(RecordComparator)> recordPQ(RecordComparator);


	while (this->input->Remove(&curRecord) == 1) {
		currentSize++;
		Record* tmpRecord = new Record;
		tmpRecord->Copy(&curRecord);

		if (bufferPage.Append(&curRecord) == 0) {
			pageCounter++;
			bufferPage.EmptyItOut();

			if (pageCounter == runlen) {
				while (!recordPQ.empty()) {
					Record* rec = new Record;
					rec = recordPQ.top();
					recordPQ.pop();
					if (bufferPage.Append(rec) == 0) {

						file.AddPage(&bufferPage, pageIndex++);
						bufferPage.EmptyItOut();
						bufferPage.Append(rec);

					}
				}
				file.AddPage(&bufferPage, pageIndex++);
				pageCounter = 0;
				runIndexes.push_back(pageIndex);
			}

			bufferPage.Append(&curRecord);
		}
		recordPQ.push(tmpRecord);
	}
	bufferPage.EmptyItOut();

	while (!recordPQ.empty()) {
		Record* rec = new Record;
		rec = recordPQ.top();
		recordPQ.pop();
		if (bufferPage.Append(rec) == 0) {
			file.AddPage(&bufferPage, pageIndex++);
			bufferPage.EmptyItOut();
		}
	}
	file.AddPage(&bufferPage, pageIndex++);
	runIndexes.push_back(pageIndex);

	bufferPage.EmptyItOut();
}


int BigQ::ExcuteMergePhase() {

	auto RunComparator = [this](Run* left, Run* right) {
		return (comp->Compare(left->nextRecordPtr, right->nextRecordPtr, order) >= 0);
	};
	priority_queue<Run*, vector<Run*>, decltype(RunComparator)> runPQ(RunComparator);

	off_t prev = 0;
	for (auto& i : runIndexes) {
		runPQ.push(new Run(&file, prev, i));
		prev = i;
	}
	if (runPQ.size() > 1) runPQ.pop();
	Run* tempRun;
	Record tempRec, * tempRec2;
	int current = 0;

	current = 0;
	// DBFileHeap tempHeapFile;
	// tempHeapFile.Create("tmp1.heap", heap, nullptr);
	while (!runPQ.empty()) {
		tempRun = runPQ.top();
		current++;

		output->Insert(tempRun->nextRecordPtr);

		if (tempRun->NextRecord() == 1) {
			// cout << "Push BACK RUN = " << current << endl;
			runPQ.push(tempRun);

		}
		cout << "current = " << current << endl;
		// tempHeapFile.Close();

		delete tempRun;
		file.Close();
		output->ShutDown();
		return 1;
	}
}

int Run::NextRecord() {

	if (bufferPage.GetFirst(nextRecordPtr) == 0) {
		curPageIdx++;
		if (curPageIdx == startPageIdx + runlen) {
			return 0;
		}
		else
		{
			bufferPage.EmptyItOut();
			file->GetPage(&bufferPage, curPageIdx);
			bufferPage.GetFirst(nextRecordPtr);
		}
	}
	return 1;
}



BigQ::BigQ(Pipe* in, Pipe* out, OrderMaker& sortorder, int runlen) {

	//Construct arguement used for worker thread
	this->input = in;
	this->output = out;
	this->order = &sortorder;
	this->runlen = runlen;
	this->comp = new ComparisonEngine();

	this->file.Open(0, "tmp.bigQ");
	this->thread = new pthread_t();
	pthread_create(thread, nullptr, Worker, (void*)this);

}

BigQ::~BigQ() {

}

Run::Run(File* file, off_t start, int pageLength) {
	this->file = file;
	this->runlen = pageLength;
	this->startPageIdx = start;
	this->curPageIdx = start;

	this->file->GetPage(&bufferPage, startPageIdx);

	this->nextRecordPtr = new Record();
	this->bufferPage.GetFirst(nextRecordPtr);
}



Record* Run::GetNextRecordPtr() {
	return nextRecordPtr;
}
