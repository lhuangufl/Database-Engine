#include "BigQ.h"

void* WorkerMain(void* arg) {
  WorkerArgs* workerArg = (WorkerArgs*)arg;
  priority_queue<Run*, vector<Run*>, RunComparator> runQueue(workerArg->order);
  priority_queue<Record*, vector<Record*>, RecordComparator> recordQueue(
      workerArg->order);
  vector<Record*> recBuff;
  Record curRecord;

  // Set disk based file for sorting
  File file;
  char* fileName = "tmp.bin";
  file.Open(0, fileName);

  // Buffer page used for disk based file
  Page bufferPage;
  int pageIndex = 0;
  int pageCounter = 0;
  // Retrieve all records from input pipe
  while (workerArg->in->Remove(&curRecord) == 1) {
    Record* tmpRecord = new Record;
    tmpRecord->Copy(&curRecord);
    // Add to another page if current page is full
    if (bufferPage.Append(&curRecord) == 0) {
      pageCounter++;
      bufferPage.EmptyItOut();

      // Add to another run if current run is full
      if (pageCounter == workerArg->runlen) {
        RecordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
        recordQueue =
            priority_queue<Record*, vector<Record*>, RecordComparator>(
                workerArg->order);
        pageCounter = 0;
      }

      bufferPage.Append(&curRecord);
    }

    recordQueue.push(tmpRecord);
  }
  // Handle the last run
  if (!recordQueue.empty()) {
    RecordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
    recordQueue = priority_queue<Record*, vector<Record*>, RecordComparator>(
        workerArg->order);
  }
  // Merge for all runs
  while (!runQueue.empty()) {
    Run* run = runQueue.top();
    runQueue.pop();
    workerArg->out->Insert(run->topRecord);
    if (run->UpdateTopRecord() == 1) {
      runQueue.push(run);
    }
  }
  file.Close();
  workerArg->out->ShutDown();
  return NULL;
}

// Used for puting records into a run, which is disk file based
void* RecordQueueToRun(
    priority_queue<Record*, vector<Record*>, RecordComparator>& recordQueue,
    priority_queue<Run*, vector<Run*>, RunComparator>& runQueue, File& file,
    Page& bufferPage, int& pageIndex) {
  bufferPage.EmptyItOut();
  int startIndex = pageIndex;
  while (!recordQueue.empty()) {
    Record* tmpRecord = new Record;
    tmpRecord->Copy(recordQueue.top());
    recordQueue.pop();
    if (bufferPage.Append(tmpRecord) == 0) {
      file.AddPage(&bufferPage, pageIndex++);
      bufferPage.EmptyItOut();
      bufferPage.Append(tmpRecord);
    }
  }
  file.AddPage(&bufferPage, pageIndex++);
  bufferPage.EmptyItOut();
  Run* run = new Run(&file, startIndex, pageIndex - startIndex);
  runQueue.push(run);
  return NULL;
}

BigQ ::BigQ(Pipe& in, Pipe& out, OrderMaker& sortorder, int runlen) {
  // read data from in pipe sort them into runlen pages

  // construct priority queue over sorted runs and dump sorted data
  // into the out pipe
  WorkerArgs* workerArgs = new WorkerArgs;
  // Construct arguement used for worker thread
  workerArgs->in = &in;
  workerArgs->out = &out;
  workerArgs->order = &sortorder;
  workerArgs->runlen = runlen;
  pthread_t worker;
  pthread_create(&worker, NULL, WorkerMain, (void*)workerArgs);
  pthread_join(worker, NULL);
  // finally shut down the out pipe
  out.ShutDown();
}

BigQ::~BigQ() {}

RecordComparator::RecordComparator(OrderMaker* order) { this->order = order; }
bool RecordComparator::operator()(Record* left, Record* right) {
  ComparisonEngine comp;
  return (comp.Compare(left, right, order) >= 0);
}

RunComparator::RunComparator(OrderMaker* order) { this->order = order; }
bool RunComparator::operator()(Run* left, Run* right) {
  ComparisonEngine comp;
  return (comp.Compare(left->topRecord, right->topRecord, order) >= 0);
}

Run::Run(File* file, int startPageIdx, int runlen) {
  this->fileBase = file;
  this->startPageIdx = startPageIdx;
  this->curPageIdx = startPageIdx;
  this->runlen = runlen;
  this->fileBase->GetPage(&bufferPage, startPageIdx);
  this->bufferPage.GetFirst(topRecord);
  UpdateTopRecord();
}

int Run::UpdateTopRecord() {
  // if bufferPage is full
  if (bufferPage.GetFirst(topRecord) == 0) {
    // if reach the last page
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
