#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>

#include <iostream>

#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

typedef struct {
  Pipe *in;
  Pipe *out;
  OrderMaker *order;
  int runlen;
} WorkerArgs;

void *WorkerMain(void *arg);

void *RecordQueueToRun(
    priority_queue<Record *, vector<Record *>, RecordComparator> &recordQ,
    priority_queue<Run *, vector<Run *>, RunComparator> &runQ, File &file,
    Page &bufferPage, int &pageIndex);

class RecordComparator {
 private:
  OrderMaker *order;

 public:
  bool operator()(Record *left, Record *right);
  RecordComparator(OrderMaker *order);
}

class RunComparator {
 private:
  OrderMaker *order;

 public:
  bool operator()(Run *left, Run *right);
  RunComparator(OrderMaker *order);
}

class BigQ {
 public:
  BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ();
};

class Run {
 private:
  File *fileBase;
  Page bufferPage;
  int startPageIdx;
  int runlen;
  int curPageIdx;
  Record *topRecord;

 public:
  Run(File *file, int startPageIdx, int runlen);
}

#endif
