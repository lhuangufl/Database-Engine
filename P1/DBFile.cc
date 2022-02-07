#include "DBFile.h"

#include <string.h>

#include <iostream>

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

DBFile::DBFile() {
	  this->file = new File();
	  this->readPage = new Page();
	  this->writePage = new Page();
	  this->currentRecord = new Record();
	  pageIndex = 1;
	  writeIndex = 1;
	  writeIsDirty = false;
	  endOfFile = false;
}

DBFile::~DBFile() {
	  delete file;
	  delete readPage;
	  delete writePage;
	  delete currentRecord;
}

int DBFile::Create(char *f_path, fType f_type, void *startup) {
	  if (f_type == heap) {
		    try {
			      this->file->Open(0, f_path);
		    } catch (const std::exception &e) {
			      std::cerr << e.what() << '\n';
			      return 0;
		    }
	  } else {
		    std::cout << f_type << " Not Implemented Yet! \n";
		    exit(1);
	  }
	  return 1;
}

void DBFile::Load(Schema &f_schema, char *loadpath) {
	  FILE *dbfile = fopen(loadpath, "r");
	  Record temp;

	  while (temp.SuckNextRecord(&f_schema, dbfile) != 0) {
		    this->Add(temp);
	  }

	  fclose(dbfile);
}

int DBFile::Open(char *f_path) {
	  try {
		    this->file->Open(1, f_path);
	  } catch (const std::exception &e) {
		    std::cerr << e.what() << '\n';
		    return 0;
	  }

	  pageIndex = 1;
	  endOfFile = false;
	  return 1;
}

void DBFile::MoveFirst() {
	  this->file->GetPage(this->readPage, 1);
	  if (this->readPage->GetFirst(this->currentRecord) == 0) {
		    std::cout << "No records in the first page \n";
		    exit(1);
	  }
}

int DBFile::Close() {
	  if (this->writeIsDirty == 1) {
		    this->file->AddPage(writePage, writeIndex);
		    writeIndex++;
	  }
	  endOfFile = true;
	  return this->file->Close();
}

void DBFile::Add(Record &rec) {
	  this->writeIsDirty = 1;

	  Record write;
	  write.Consume(&rec);

	  if (writePage->Append(&write) == 0) {
		    this->file->AddPage(writePage, writeIndex);
		    writeIndex++;
		    this->writePage->EmptyItOut();
		    writePage->Append(&write);
	  }
}

int DBFile::GetNext(Record &fetchme) {
	  if (!endOfFile) {
		    fetchme.Copy(this->currentRecord);
		    if (this->readPage->GetFirst(this->currentRecord) == 0) {
			      pageIndex++;
			      if (pageIndex > this->file->GetLength()) {
				        endOfFile = true;
				        std::cout << "Page Index Exceeds File Length"
					        << std::endl;
				        return 0;
			      } else {
				        this->file->GetPage(this->readPage, pageIndex);
			      }
		    } else {
			      std::cout << "currentRecord is the first one" << std::endl;
		    }
		    return 1;
	  } else {
		    std::cout << "Already at the end of the file" << std::endl;
		    return 0;
	  }
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	  ComparisonEngine compare;
	  int result1 = 0;
	  int result2 = 1;
	  // Record temp2;

	  while (result1 == 0 && result2 != 0) {
		    result2 = this->GetNext(fetchme);
		    result1 = compare.Compare(&fetchme, &literal, &cnf);
	  }

	  if (result2 == 0) return 0;

	  if (result1 == 1) return 1;

	  return 0;
}
