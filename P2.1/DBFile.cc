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
	pageIndex = 0;
	writeIndex = 0;
	writeIsDirty = false;
	endOfFile = false;
}

DBFile::~DBFile() {
	delete file;
	delete readPage;
	delete writePage;
	delete currentRecord;
}

int DBFile::Create(char* f_path, fType f_type, void* startup) {
	if (f_type == heap) {
		try {
			this->file->Open(0, f_path);
		}
		catch (const std::exception& e) {
			std::cerr << e.what() << '\n';
			return 0;
		}
		readPage->EmptyItOut();
		writePage->EmptyItOut();
	}
	else {
		std::cout << f_type << " Not Implemented Yet! \n";
		return 0;
	}
	return 1;
}

void DBFile::Load(Schema& f_schema, char* loadpath) {
	FILE* dbfile = fopen(loadpath, "r");
	Record temp;
	Page* page = new Page();
	this->pageIndex = 0;
	while (temp.SuckNextRecord(&f_schema, dbfile)) {
		if (page->Append(&temp) == 0) {
			this->file->AddPage(page, this->pageIndex + 1);
			this->pageIndex++;
			page->EmptyItOut();
			page->Append(&temp);
		}
	}
	this->file->AddPage(page, this->pageIndex + 1);
	page->EmptyItOut();

	fclose(dbfile);
}

int DBFile::Open(char* f_path) {
	try {
		this->file->Open(1, f_path);
	}
	catch (const std::exception& e) {
		std::cerr << e.what() << '\n';
		return 0;
	}
	this->pageIndex = 1;
	endOfFile = false;
	return 1;
}

void DBFile::MoveFirst() {
	if (this->file->GetLength() < 1) {
		std::cerr << "DBFile::MoveFirst() : No Data in File" << std::endl;
		exit(1);
	}
	this->file->GetPage(this->readPage, pageIndex);	 // get the first page
	if (this->readPage->GetFirst(this->currentRecord) == 0) {
		std::cout << "No records in the first page \n";
		this->endOfFile = true;
	}
}

int DBFile::Close() {
	if (this->writeIsDirty) {
		this->file->AddPage(writePage, writeIndex);
		this->writeIndex++;
	}
	endOfFile = true;
	return this->file->Close();
}

void DBFile::Add(Record& rec) {
	this->writeIsDirty = 1;
	this->file->GetPage(this->writePage, this->file->GetLength() - 1);
	Record write;
	write.Consume(&rec);

	if (writePage->Append(&write) == 0) {
		this->file->AddPage(writePage, writeIndex);
		writeIndex++;
		this->writePage->EmptyItOut();
		writePage->Append(&write);
	}
}

int DBFile::GetNext(Record& fetchme) {
	if (!endOfFile) {
		fetchme.Copy(this->currentRecord);
		if (this->readPage->GetFirst(this->currentRecord) == 0) {
			pageIndex++;
			if (pageIndex >= this->file->GetLength() - 1) {
				endOfFile = true;
			}
			else {
				this->file->GetPage(this->readPage, pageIndex);
				this->readPage->GetFirst(this->currentRecord);
			}
		}
		return 1;
	}
	return 0;
}

int DBFile::GetNext(Record& fetchme, CNF& cnf, Record& literal) {
	ComparisonEngine compare;

	while (this->GetNext(fetchme)) {
		if (compare.Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}
	}
	return 0;
}
