#ifndef TYPEDEFS_H
#define TYPEDEFS_H

#include "../Top_Layer/record_type.h"
#include "../Top_Layer/condition_op_const.h"

#define NOKEY 0
#define PRIKEY 1
#define SECKEY 2
#define SEC_INDEX_ORDER 15
#define SEC_INDEX_BLOCK 2*sizeof(int)+(SEC_INDEX_ORDER+1)*sizeof(blk_addr)+(SEC_INDEX_ORDER-1)*sizeof(blk_addr)
#define MAX_RECORDS INT_MAX
#define INT_INDEX_BLOCK 2*sizeof(int)+(SEC_INDEX_ORDER+1)*sizeof(blk_addr)+(SEC_INDEX_ORDER-1)*sizeof(int)
#define FLOAT_INDEX_BLOCK 2*sizeof(int)+(SEC_INDEX_ORDER+1)*sizeof(blk_addr)+(SEC_INDEX_ORDER-1)*sizeof(float)
#define STRING_INDEX_BLOCK 2*sizeof(int)+(SEC_INDEX_ORDER+1)*sizeof(blk_addr)+(SEC_INDEX_ORDER-1)*104


struct attribute_type{
	column_type::data_type type;
	blk_size size;
	string name;
	blk_addr rid;
	int ifindex;
};

struct mystr{
	char str[100];
	bool operator < (const mystr& other){
		return strncmp(str, other.str, 100) < 0;
	}
	bool operator > (const mystr& other){
		return strncmp(str, other.str, 100) > 0;
	}
	bool operator <= (const mystr& other){
		return strncmp(str, other.str, 100) <= 0;
	}
	bool operator >= (const mystr& other){
		return strncmp(str, other.str, 100) >= 0;
	}
	bool operator == (const mystr& other){
		return strncmp(str, other.str, 100) == 0;
	}
	bool operator != (const mystr& other){
		return strncmp(str, other.str, 100);
	}
	ostream& operator<<(ostream& os){
	    os << str << endl;
	    return os;
	}
	//friend ostream& operator<<(ostream& os, const mystr& st);
};
/*
ostream& operator<<(ostream& os, const mystr& st){
    os << st.str << endl;
    return os;
}
*/

typedef vector<attribute_type> schema_type;
typedef map<string, schema_type> catalog_type;
typedef map<string, blk_size> recordsize;
typedef pair<column_type::data_type, void*> data;
typedef vector<data> entry_type;
typedef vector<updateInfo> update_vector;
typedef pair<int, blk_addr> iindex;
typedef pair<float, blk_addr> findex;
typedef pair<mystr, blk_addr> sindex;

#endif