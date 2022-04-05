
CC = g++ -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

all: test.out gTest.out test_p1.out test_p2.out

# test.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o DBFileGeneric.o DBFileHeap.o DBFileSorted.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
# 	$(CC) -o test.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o DBFileGeneric.o DBFileHeap.o DBFileSorted.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread

test.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o test.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread

gtest.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gTest.o
	$(CC) -o gtest.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gTest.o -lfl -lpthread -lgtest

test_p1.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p1.o
	$(CC) -o test_p1.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p1.o -lfl -lpthread

test_p2.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p2.o
	$(CC) -o test_p2.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p2.o -lfl -lpthread


test.o: test.cc
	$(CC) -g -c test.cc

gTest.o: gTest.cc
	$(CC) -g -c gTest.cc

test_p1.o: test_p1.cc
	$(CC) -g -c test_p1.cc

test_p2.o: test_p2.cc
	$(CC) -g -c test_p2.cc

Util.o: Util.cc
	$(CC) -g -c Util.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

DBFileGeneric.o: DBFileGeneric.cc
	$(CC) -g -c DBFileGeneric.cc

DBFileHeap.o: DBFileHeap.cc
	$(CC) -g -c DBFileHeap.cc

DBFileSorted.o: DBFileSorted.cc
	$(CC) -g -c DBFileSorted.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
	rm -f *.tmp
