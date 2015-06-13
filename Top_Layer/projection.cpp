#include "projection.h"
#include <cstring>

#define AS_INT(x) (*(int*)(&(x)))
#define AS_FLOAT(x) (*(float*)(&(x)))
#define AS_STRING(x) ((char*)(&(x)))

projection::projection(){
	enabled = false;
}

void projection::setup(){
	if (!enabled) return;

	for (vector<pair<string, string> >::iterator i = columns.begin(); i != columns.end(); i++){
		int offset;
		if ((offset = offset_of_column(input_record_type, i->first, i->second)) == -1)
			throw i->first + string(".") + i->second + string(" not in scope");
		int idx = index_of_column(input_record_type, i->first, i->second);
		offsets.push_back(offset);
		output_record_type.push_back(input_record_type[idx]);
	}

	output_record_size = record_size(output_record_type);

}

void projection::project(record& r){
	if (!enabled) return;

	record rold = r;
	r.resize(output_record_size);

	int cur=0;

	for (record_type::iterator i = output_record_type.begin(); i != output_record_type.end(); i++){
		if (i->type == column_type::INT){
			AS_INT(r[cur]) = AS_INT(rold[offsets[i-output_record_type.begin()]]);
		}
		else if (i->type == column_type::FLOAT){
			AS_FLOAT(r[cur]) = AS_FLOAT(rold[offsets[i-output_record_type.begin()]]);
		}
		else {
			strncpy(AS_STRING(r[cur]), AS_STRING(rold[offsets[i-output_record_type.begin()]]), i->size);
		}
		cur += i->size;
	}

}

record_type projection::getOutputRecordType(){
	if (!enabled) return input_record_type;
	return output_record_type;
}