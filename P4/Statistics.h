// #ifndef STATISTICS_
// #define STATISTICS_
// #include "ParseTree.h"
//
//
// class Statistics
// {
// public:
// 	Statistics();
// 	Statistics(Statistics &copyMe);	 // Performs deep copy
// 	~Statistics();
//
//
// 	void AddRel(char *relName, int numTuples);
// 	void AddAtt(char *relName, char *attName, int numDistincts);
// 	void CopyRel(char *oldName, char *newName);
//
// 	void Read(char *fromWhere);
// 	void Write(char *fromWhere);
//
// 	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
// 	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
//
// };
//
// #endif

#ifndef STATISTICS_H
#define STATISTICS_H
#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;

class AttributeInfo;
class RelationInfo;

typedef map<string, AttributeInfo> AttrMap;
typedef map<string, RelationInfo> RelMap;

class AttributeInfo {

public:

	string attrName;
	int distinctTuples;

	AttributeInfo();
	AttributeInfo(string name, int num);
	AttributeInfo(const AttributeInfo& copyMe);

	AttributeInfo& operator= (const AttributeInfo& copyMe);

};

class RelationInfo {

public:

	double numTuples;

	bool isJoint;
	string relName;

	AttrMap attrMap;
	map<string, string> relJoint;

	RelationInfo();
	RelationInfo(string name, int tuples);
	RelationInfo(const RelationInfo& copyMe);

	RelationInfo& operator= (const RelationInfo& copyMe);

	bool isRelationPresent(string relName);

};

class Statistics {

private:

	double AndOp(AndList* andList, char* relName[], int numJoin);
	double OrOp(OrList* orList, char* relName[], int numJoin);
	double ComOp(ComparisonOp* comOp, char* relName[], int numJoin);
	int GetRelForOp(Operand* operand, char* relName[], int numJoin, RelationInfo& relInfo);

public:

	RelMap relMap;

	Statistics();
	Statistics(Statistics& copyMe);	 // Performs deep copy
	~Statistics();

	Statistics operator= (Statistics& copyMe);

	void AddRel(char* relName, int numTuples);
	void AddAtt(char* relName, char* attrName, int numDistincts);
	void CopyRel(char* oldName, char* newName);

	void Read(char* fromWhere);
	void Write(char* toWhere);

	void  Apply(struct AndList* parseTree, char* relNames[], int numToJoin);
	double Estimate(struct AndList* parseTree, char** relNames, int numToJoin);

	bool isRelInMap(string relName, RelationInfo& relInfo);

};

#endif
