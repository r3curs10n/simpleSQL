#ifndef H_PROJECTION
#define H_PROJECTION

#include "record_type.h"
#include <set>
#include <string>
using namespace std;

class projection {

private:
	vector<int> offsets;

public:

	record_type input_record_type;
	record_type output_record_type;
	vector<pair<string, string> > columns;
	bool enabled;
	int output_record_size;

	void setup();

	void project(record& r);

	record_type getOutputRecordType();

	projection();

};

#endif