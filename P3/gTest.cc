#include "gTest.h"


Attribute IA = { "int", Int };
Attribute SA = { "string", String };
Attribute DA = { "double", Double };

int clear_pipe(Pipe& in_pipe, Schema* schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove(&rec)) {
		if (print) {
			rec.Print(schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe(Pipe& in_pipe, Schema* schema, Function& func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove(&rec)) {
		if (print) {
			rec.Print(schema);
		}
		int ival = 0; double dval = 0;
		func.Apply(rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}
int pipesz = 10000; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
Pipe _ps(pipesz), _p(pipesz), _s(pipesz), _o(pipesz), _li(pipesz), _c(pipesz);
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
Function func_ps, func_p, func_s, func_o, func_li, func_c;

int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;

void init_SF_ps(char* pred_str, int numpgs) {
	dbf_ps.Open(ps->path());
	get_cnf(pred_str, ps->schema(), cnf_ps, lit_ps);
	SF_ps.Use_n_Pages(numpgs);
}

void init_SF_p(char* pred_str, int numpgs) {
	dbf_p.Open(p->path());
	get_cnf(pred_str, p->schema(), cnf_p, lit_p);
	SF_p.Use_n_Pages(numpgs);
}

void init_SF_s(char* pred_str, int numpgs) {
	dbf_s.Open(s->path());
	get_cnf(pred_str, s->schema(), cnf_s, lit_s);
	SF_s.Use_n_Pages(numpgs);
}

void init_SF_o(char* pred_str, int numpgs) {
	dbf_o.Open(o->path());
	get_cnf(pred_str, o->schema(), cnf_o, lit_o);
	SF_o.Use_n_Pages(numpgs);
}

void init_SF_li(char* pred_str, int numpgs) {
	dbf_li.Open(li->path());
	get_cnf(pred_str, li->schema(), cnf_li, lit_li);
	SF_li.Use_n_Pages(numpgs);
}

void init_SF_c(char* pred_str, int numpgs) {
	dbf_c.Open(c->path());
	get_cnf(pred_str, c->schema(), cnf_c, lit_c);
	SF_c.Use_n_Pages(numpgs);
}

TEST(RelOp, SelectFile) {
	setup();
	char* pred_ps = "(ps_supplycost < 1.03)";
	init_SF_ps(pred_ps, 100);
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone();
	int cnt = clear_pipe(_ps, ps->schema(), false);
	dbf_ps.Close();
	cleanup();
	ASSERT_EQ(cnt, 21);
}

TEST(RelOp, SelectFile2) {
	setup();
	char* pred_ps = "(ps_supplycost < 2.03)";
	init_SF_ps(pred_ps, 100);
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone();
	int cnt = clear_pipe(_ps, ps->schema(), false);
	dbf_ps.Close();
	cleanup();
	ASSERT_EQ(cnt, 840);
}

TEST(RelOp, Project) {
	setup();
	char* pred_p = "(p_retailprice > 100.00) AND (p_retailprice < 931.3)";
	init_SF_p(pred_p, 100);

	Project P_p;
	Pipe _out(pipesz);
	int keepMe[] = { 0,1,7 };
	int numAttsIn = pAtts;
	int numAttsOut = 3;
	P_p.Use_n_Pages(buffsz);

	SF_p.Run(dbf_p, _p, cnf_p, lit_p);
	P_p.Run(_p, _out, keepMe, numAttsIn, numAttsOut);

	SF_p.WaitUntilDone();
	P_p.WaitUntilDone();

	Attribute att3[] = { IA, SA, DA };
	Schema out_sch("out_sch", numAttsOut, att3);
	int cnt = clear_pipe(_out, &out_sch, false);

	dbf_p.Close();
	cleanup();
	ASSERT_EQ(cnt, 527);
}



TEST(RelOp, GroupBy) {
	setup();
	char* pred_s = "(s_suppkey = s_suppkey)";
	init_SF_s(pred_s, 100);
	SF_s.Run(dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

	char* pred_ps = "(ps_suppkey = ps_suppkey)";
	init_SF_ps(pred_ps, 100);

	Join J;
	Pipe _s_ps(pipesz);
	CNF cnf_p_ps;
	Record lit_p_ps;
	get_cnf("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

	int numAttsLeft = sAtts;
	int numAttsRight = psAtts;
	int attsToKeep[] = { 0, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4 };
	int numAttsToKeep = sAtts + psAtts;
	int startOfRight = sAtts;

	int outAtts = sAtts + psAtts;
	Attribute s_nationkey = { "s_nationkey", Int };
	Attribute ps_supplycost = { "ps_supplycost", Double };
	Attribute joinatt[] = { IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA };
	Schema join_sch("join_sch", outAtts, joinatt);

	GroupBy G;
	Pipe _out(100);
	Function func;
	char* str_sum = "(ps_supplycost)";
	get_cnf(str_sum, &join_sch, func);
	func.Print();

	string numAtts = "1";
	string atts = "3";
	string types = "Int";
	OrderMaker grp_order(numAtts, atts, types);
	G.Use_n_Pages(200);

	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	J.Run(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
	G.Run(_s_ps, _out, grp_order, func);

	SF_s.WaitUntilDone();
	SF_ps.WaitUntilDone();
	J.WaitUntilDone();
	G.WaitUntilDone();

	Attribute group_sum [2] = { DA, s_nationkey };
	Schema sum_sch("sum_sch", 2, group_sum);
	int cnt = clear_pipe(_out, &sum_sch, false);

	ASSERT_EQ(cnt, 25);
}



int main(int argc, char** argv) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
