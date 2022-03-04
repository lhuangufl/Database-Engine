#include "BigQ.h"

void* Worker(void* bigQ) {

	auto* bigq = (BigQ*)bigQ;

	bigq->ExcuteSortPhase();
	return nullptr;

	//Set disk based file for sorting
}

int BigQ::ExcuteSortPhase() {
	File file;
	char* fileName = "tmp.bin";
	file.Open(0, fileName);

	//Buffer page used for disk based file
	Page bufferPage;
	Record curRecord;
	int pageIndex = 0;
	int pageCounter = 0;

	auto RecordComparator = [this](Record* left, Record* right) {
		return (comp->Compare(left, right, order) >= 0);
	};
	auto RunComparator = [this](Run* left, Run* right) {
		return (comp->Compare(left->topRecord, right->topRecord, order) >= 0);
	};
	//Retrieve all records from input pipe
	priority_queue<Record*, vector<Record*>, decltype(RecordComparator)> recordPQ(RecordComparator);
	priority_queue<Run*, vector<Run*>, decltype(RunComparator)> runPQ(RunComparator);
	while (this->input->Remove(&curRecord) == 1) {
		std::cout << "Remove 1 record from Input Piepe; Wait for next record" << std::endl;
		Record* tmpRecord = new Record;
		tmpRecord->Copy(&curRecord);
		//Add to another page if current page is full
		if (bufferPage.Append(&curRecord) == 0) {
			pageCounter++;
			bufferPage.EmptyItOut();

			//Add to another run if current run is full
			if (pageCounter == runlen) {

				//Used for take sequences of pages of records, and construct a run to hold such records, and put run
				//into priority queue
				bufferPage.EmptyItOut();
				int startIndex = pageIndex;
				while (!recordPQ.empty()) {
					Record* tmpRecord = new Record;
					tmpRecord->Copy(recordPQ.top());
					recordPQ.pop();
					if (bufferPage.Append(tmpRecord) == 0) {
						file.AddPage(&bufferPage, pageIndex++);
						bufferPage.EmptyItOut();
						bufferPage.Append(tmpRecord);
					}
				}
				file.AddPage(&bufferPage, pageIndex++);
				bufferPage.EmptyItOut();
				runPQ.push(new Run(&file, startIndex, pageIndex - startIndex));
				//Used for puting records into a run, which is disk file based
				// Clean up Records in recordPQ
				while (!recordPQ.empty()) { recordPQ.pop(); }
				pageCounter = 0;
			}

			bufferPage.Append(&curRecord);
		}

		recordPQ.push(tmpRecord);
	}
	// Handle the last run
	if (!recordPQ.empty()) {
		//Used for take sequences of pages of records, and construct a run to hold such records, and put run
		//into priority queue
		bufferPage.EmptyItOut();
		int startIndex = pageIndex;
		while (!recordPQ.empty()) {
			Record* tmpRecord = new Record;
			tmpRecord->Copy(recordPQ.top());
			recordPQ.pop();
			if (bufferPage.Append(tmpRecord) == 0) {
				file.AddPage(&bufferPage, pageIndex++);
				bufferPage.EmptyItOut();
				bufferPage.Append(tmpRecord);
			}
		}
		file.AddPage(&bufferPage, pageIndex++);
		bufferPage.EmptyItOut();
		runPQ.push(new Run(&file, startIndex, pageIndex - startIndex));
		//Used for puting records into a run, which is disk file based
		while (!recordPQ.empty()) { recordPQ.pop(); }
	}
	while (!runPQ.empty()) {
		Run* run = runPQ.top();
		runPQ.pop();
		this->output->Insert(run->topRecord);
		if (run->UpdateTopRecord() == 1) {
			runPQ.push(run);
		}
	}
	file.Close();
	output->ShutDown();
	return 1;
}



BigQ::BigQ(Pipe& in, Pipe& out, OrderMaker& sortorder, int runlen) {
	pthread_t worker;
	//Construct arguement used for worker thread
	this->input = &in;
	this->output = &out;
	this->order = &sortorder;
	this->runlen = runlen;
	this->comp = new ComparisonEngine();

	pthread_create(&worker, nullptr, Worker, (void*)this);
	pthread_join(worker, NULL);
	out.ShutDown();
}

BigQ::~BigQ() {

}

Run::Run(File* file, int start, int length) {
	fileBase = file;
	runlen = length;
	startPageIdx = start;
	curPageIdx = start;
	fileBase->GetPage(&bufferPage, startPageIdx);
	topRecord = new Record;

	UpdateTopRecord();
}

int Run::UpdateTopRecord() {
	//if bufferPage is full
	if (bufferPage.GetFirst(topRecord) == 0) {
		//if reach the last page
		curPageIdx++;
		if (curPageIdx == startPageIdx + runlen) {
			return 0;
		}
		bufferPage.EmptyItOut();
		fileBase->GetPage(&bufferPage, curPageIdx);
		bufferPage.GetFirst(topRecord);
	}
	return 1;
}


