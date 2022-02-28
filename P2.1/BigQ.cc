#include "BigQ.h"

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
  // read data from in pipe sort them into runlen pages

  // construct priority queue over sorted runs and dump sorted data
  // into the out pipe
  pthread_t worker;
  WorkerArg *workerArg = new WorkerArg;
  // Construct arguement used for worker thread
  workerArg->in = &in;
  workerArg->out = &out;
  workerArg->order = &sortorder;
  workerArg->runlen = runlen;
  pthread_create(&worker, NULL, workerMain, (void *)workerArg);
  pthread_join(worker, NULL);
  // finally shut down the out pipe
  out.ShutDown();
}

BigQ::~BigQ() {}

RecordComparator::RecordComparator(OrderMaker *order) { this->order = order; }
RecordComparator::operator()(Record *left, Record *right) {
  ComparisonEngine comp;
  return comp.Compare(left, right, order) >= 0;
}

RunComparator::RunComparator(OrderMaker *order) { this->order = order; }
RunComparator::operator()(Run *left, Run *right) {
  ComparisonEngine comp;
  return comp.Compare(left->topRecord, right->topRecord, order) >= 0;
}

Run::Run(File *file, int startPageIdx, int runlen) {
  this->fileBase = file;
  this->startPageIdx = startPageIdx;
  this->runlen = runlen;
  this->curPageIdx = startPageIdx;
  this->fileBase->GetPage(&bufferPage, startPageIdx);
  this->bufferPage.GetFirst(topRecord);
}
