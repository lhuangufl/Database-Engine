CC = g++ -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

all: a42.out gTest.out #tt.out#test_p1.out test_p2.out# test_p3.out

a42.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryOptimizer.o y.tab.o lex.yy.o test.o
	$(CC) -o a42.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryOptimizer.o y.tab.o lex.yy.o test.o -lfl -lpthread

gTest.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryOptimizer.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gTest.o
	$(CC) -o gTest.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryOptimizer.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gTest.o -lfl -lpthread -lgtest

test.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryOptimizer.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o tt.o
	$(CC) -o test.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryOptimizer.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o tt.o -lfl -lpthread -lgtest

test_p1.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p1.o
	$(CC) -o test_p1.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p1.o -lfl -lpthread

test_p2.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p2.o
	$(CC) -o test_p2.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p2.o -lfl -lpthread

test_p3.out: Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p3.o
	$(CC) -o test_p3.out Util.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFBase.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p3.o -lfl -lpthread

test.o: test.cc
	$(CC) -g -c test.cc

gTest.o: gTest.cc
	$(CC) -g -c gTest.cc

tt.o: tt.cc
	$(CC) -g -c tt.cc

test_p1.o: test_p1.cc
	$(CC) -g -c test_p1.cc

test_p2.o: test_p2.cc
	$(CC) -g -c test_p2.cc

test_p3.o: test_p3.cc
	$(CC) -g -c test_p3.cc

QueryOptimizer.o: QueryOptimizer.cc
	$(CC) -g -c QueryOptimizer.cc

ParseTree.o: ParseTree.cc
	$(CC) -g -c ParseTree.cc

Util.o: Util.cc
	$(CC) -g -c Util.cc

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o y.tab.o lex.yy.o test.o -lfl

test.o: test.cc
	$(CC) -g -c test.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFBase.o: DBFBase.cc
	$(CC) -g -c DBFBase.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

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
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f *.stat
