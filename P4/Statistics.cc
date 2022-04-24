// #include "Statistics.h"
//
// Statistics::Statistics()
// {
// }
// Statistics::Statistics(Statistics &copyMe)
// {
// }
// Statistics::~Statistics()
// {
// }
//
// void Statistics::AddRel(char *relName, int numTuples)
// {
// }
// void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
// {
// }
// void Statistics::CopyRel(char *oldName, char *newName)
// {
// }
//
// void Statistics::Read(char *fromWhere)
// {
// }
// void Statistics::Write(char *fromWhere)
// {
// }
//
// void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
// {
// }
// double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
// {
// }

#include "Statistics.h"

AttributeInfo::AttributeInfo() {}

AttributeInfo::AttributeInfo(string name, int num) {

        attrName = name;
        distinctTuples = num;

}

AttributeInfo::AttributeInfo(const AttributeInfo& copyMe) {

        attrName = copyMe.attrName;
        distinctTuples = copyMe.distinctTuples;

}

AttributeInfo& AttributeInfo :: operator= (const AttributeInfo& copyMe) {

        attrName = copyMe.attrName;
        distinctTuples = copyMe.distinctTuples;

        return *this;

}

RelationInfo::RelationInfo() {

        isJoint = false;

}

RelationInfo::RelationInfo(string name, int tuples) {

        isJoint = false;
        relName = name;
        numTuples = tuples;

}

RelationInfo::RelationInfo(const RelationInfo& copyMe) {

        isJoint = copyMe.isJoint;
        relName = copyMe.relName;
        numTuples = copyMe.numTuples;
        attrMap.insert(copyMe.attrMap.begin(), copyMe.attrMap.end());

}

RelationInfo& RelationInfo :: operator= (const RelationInfo& copyMe) {

        isJoint = copyMe.isJoint;
        relName = copyMe.relName;
        numTuples = copyMe.numTuples;
        attrMap.insert(copyMe.attrMap.begin(), copyMe.attrMap.end());

        return *this;

}

bool RelationInfo::isRelationPresent(string relName) {

        if (relName == relName) {

                return true;

        }

        auto it = relJoint.find(relName);

        if (it != relJoint.end()) {

                return true;

        }

        return false;

}

Statistics::Statistics() {}

Statistics::Statistics(Statistics& copyMe) {

        relMap.insert(copyMe.relMap.begin(), copyMe.relMap.end());

}

Statistics :: ~Statistics() {}

Statistics Statistics :: operator= (Statistics& copyMe) {

        relMap.insert(copyMe.relMap.begin(), copyMe.relMap.end());

        return *this;

}

double Statistics::AndOp(AndList* andList, char* relName[], int numJoin) {

        if (andList == NULL) {

                return 1.0;

        }

        double left = 1.0;
        double right = 1.0;

        left = OrOp(andList->left, relName, numJoin);
        right = AndOp(andList->rightAnd, relName, numJoin);

        //	cout << "left of and : " << left << endl;
        //	cout << "right of and : " << right << endl;

        return left * right;

}

double Statistics::OrOp(OrList* orList, char* relName[], int numJoin) {

        if (orList == NULL) {

                return 0.0;

        }

        double left = 0.0;
        double right = 0.0;

        left = ComOp(orList->left, relName, numJoin);

        int count = 1;

        OrList* temp = orList->rightOr;
        char* attrName = orList->left->left->value;

        while (temp) {

                if (strcmp(temp->left->left->value, attrName) == 0) {

                        count++;

                }

                temp = temp->rightOr;

        }

        if (count > 1) {

                return (double)count * left;

        }

        right = OrOp(orList->rightOr, relName, numJoin);

        //	cout << "Left of Or : " << left << endl;
        //	cout << "Right of Or : " << right << endl;

        return (double)(1.0 - (1.0 - left) * (1.0 - right));

}

double Statistics::ComOp(ComparisonOp* compOp, char* relName[], int numJoin) {

        RelationInfo leftRel, rightRel;

        double left = 0.0;
        double right = 0.0;

        int leftResult = GetRelForOp(compOp->left, relName, numJoin, leftRel);
        int rightResult = GetRelForOp(compOp->right, relName, numJoin, rightRel);
        int code = compOp->code;

        if (compOp->left->code == NAME) {

                if (leftResult == -1) {

                        cout << compOp->left->value << " does not belong to any relation!" << endl;
                        left = 1.0;

                }
                else {

                        string buffer(compOp->left->value);

                        left = leftRel.attrMap [buffer].distinctTuples;

                }

        }
        else {

                left = -1.0;

        }

        if (compOp->right->code == NAME) {

                if (rightResult == -1) {

                        cout << compOp->right->value << " does not belong to any relation!" << endl;
                        right = 1.0;

                }
                else {

                        string buffer(compOp->right->value);

                        right = rightRel.attrMap [buffer].distinctTuples;

                }

        }
        else {

                right = -1.0;

        }

        if (code == LESS_THAN || code == GREATER_THAN) {

                return 1.0 / 3.0;

        }
        else if (code == EQUALS) {

                if (left > right) {

                        return 1.0 / left;

                }
                else {

                        return 1.0 / right;

                }

        }

        cout << "Error!" << endl;
        return 0.0;

}

int Statistics::GetRelForOp(Operand* operand, char* relName[], int numJoin, RelationInfo& relInfo) {

        if (operand == NULL) {

                return -1;

        }

        if (relName == NULL) {

                return -1;

        }

        for (auto iter = relMap.begin(); iter != relMap.end(); iter++) {

                string buffer(operand->value);

                if (iter->second.attrMap.find(buffer) != iter->second.attrMap.end()) {

                        relInfo = iter->second;

                        return 0;

                }

        }

        return -1;

}

void Statistics::AddRel(char* relName, int numTuples) {

        string relStr(relName);

        RelationInfo temp(relStr, numTuples);

        relMap [relStr] = temp;

}

void Statistics::AddAtt(char* relName, char* attrName, int numDistincts) {

        string relStr(relName);
        string attrStr(attrName);

        AttributeInfo temp(attrStr, numDistincts);

        relMap [relStr].attrMap [attrStr] = temp;

}

void Statistics::CopyRel(char* oldName, char* newName) {

        string oldStr(oldName);
        string newStr(newName);

        relMap [newStr] = relMap [oldStr];
        relMap [newStr].relName = newStr;

        AttrMap newAttrMap;

        for (
                auto it = relMap [newStr].attrMap.begin();
                it != relMap [newStr].attrMap.end();
                it++
                ) {

                string newAttrStr = newStr;
                newAttrStr.append(".");
                newAttrStr.append(it->first);

                AttributeInfo temp(it->second);
                temp.attrName = newAttrStr;

                newAttrMap [newAttrStr] = temp;

        }

        relMap [newStr].attrMap = newAttrMap;

}

void Statistics::Read(char* fromWhere) {

        int numRel, numJoint, numAttr, numTuples, numDistincts;
        string relName, jointName, attrName;

        ifstream in(fromWhere);

        if (!in) {

                cout << "File \"" << fromWhere << "\" does not exist!" << endl;

        }

        relMap.clear();

        in >> numRel;

        for (int i = 0; i < numRel; i++) {

                in >> relName;
                in >> numTuples;

                RelationInfo relation(relName, numTuples);
                relMap [relName] = relation;

                in >> relMap [relName].isJoint;

                if (relMap [relName].isJoint) {

                        in >> numJoint;

                        for (int j = 0; j < numJoint; j++) {

                                in >> jointName;

                                relMap [relName].relJoint [jointName] = jointName;

                        }

                }

                in >> numAttr;

                for (int j = 0; j < numAttr; j++) {

                        in >> attrName;
                        in >> numDistincts;

                        AttributeInfo attrInfo(attrName, numDistincts);

                        relMap [relName].attrMap [attrName] = attrInfo;

                }

        }

}

void Statistics::Write(char* toWhere) {

        ofstream out(toWhere);

        out << relMap.size() << endl;

        for (
                auto iter = relMap.begin();
                iter != relMap.end();
                iter++
                ) {

                out << iter->second.relName << endl;
                out << iter->second.numTuples << endl;
                out << iter->second.isJoint << endl;

                if (iter->second.isJoint) {

                        out << iter->second.relJoint.size() << endl;

                        for (
                                auto it = iter->second.relJoint.begin();
                                it != iter->second.relJoint.end();
                                it++
                                ) {

                                out << it->second << endl;

                        }

                }

                out << iter->second.attrMap.size() << endl;

                for (
                        auto it = iter->second.attrMap.begin();
                        it != iter->second.attrMap.end();
                        it++
                        ) {

                        out << it->second.attrName << endl;
                        out << it->second.distinctTuples << endl;

                }

        }

        out.close();

}

void Statistics::Apply(struct AndList* parseTree, char* relNames[], int numToJoin) {

        int index = 0;
        int numJoin = 0;
        char* names [100];

        RelationInfo rel;

        while (index < numToJoin) {

                string buffer(relNames [index]);

                auto iter = relMap.find(buffer);

                if (iter != relMap.end()) {

                        rel = iter->second;

                        names [numJoin++] = relNames [index];

                        if (rel.isJoint) {

                                int size = rel.relJoint.size();

                                if (size <= numToJoin) {

                                        for (int i = 0; i < numToJoin; i++) {

                                                string buf(relNames [i]);

                                                if (rel.relJoint.find(buf) == rel.relJoint.end() &&
                                                        rel.relJoint [buf] != rel.relJoint [buffer]) {

                                                        cout << "Cannot be joined!" << endl;

                                                        return;

                                                }

                                        }

                                }
                                else {

                                        cout << "Cannot be joined!" << endl;

                                }

                        }
                        else {

                                index++;

                                continue;

                        }

                }
                else {

                        // cout << buffer << " Not Found!" << endl;

                }

                index++;

        }

        double estimation = Estimate(parseTree, names, numJoin);

        index = 1;
        string firstRelName(names [0]);
        RelationInfo firstRel = relMap [firstRelName];
        RelationInfo temp;

        relMap.erase(firstRelName);
        firstRel.isJoint = true;
        firstRel.numTuples = estimation;

        //	cout << firstRelName << endl;
        //	cout << estimation << endl;

        while (index < numJoin) {

                string buffer(names [index]);

                firstRel.relJoint [buffer] = buffer;
                temp = relMap [buffer];
                relMap.erase(buffer);

                firstRel.attrMap.insert(temp.attrMap.begin(), temp.attrMap.end());
                index++;

        }

        relMap.insert(pair<string, RelationInfo>(firstRelName, firstRel));

}

double Statistics::Estimate(struct AndList* parseTree, char** relNames, int numToJoin) {

        int index = 0;

        double factor = 1.0;
        double product = 1.0;

        while (index < numToJoin) {

                string buffer(relNames [index]);

                if (relMap.find(buffer) != relMap.end()) {

                        // cout << buffer << " Found in Estimate!" << endl;
                        product *= (double)relMap [buffer].numTuples;

                }
                else {

                        // cout << buffer << " Not Found!" << endl;

                }

                index++;

        }

        if (parseTree == NULL) {

                return product;

        }

        factor = AndOp(parseTree, relNames, numToJoin);

        //	cout << product << endl;
        //	cout << factor << endl;

        return factor * product;

}
