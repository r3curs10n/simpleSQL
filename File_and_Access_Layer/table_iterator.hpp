#ifndef TABLE_ITERATOR_H
#define TABLE_ITERATOR_H

#include "../Space_and_Buffer_Manager/file.h"
#include "bptree.hpp"
#include "typedefs.hpp"
#include "table_scanner.hpp"

//iterator to scan through records
template <class keytype> class table_iterator : public table_scanner{
	file *tfile;	//table file object pointer
	file *inxfile;	//index file object pointer
	blk_size bsize;
	bool flag;
	bool init;
	bptree<keytype> inx;
	keytype *rkeys;
	blk_addr *rpointers;
	keytype start;
	keytype end;
	int num;
	int pos;
public:
	table_iterator(file *fp, file *ifp, blk_size bs);
	table_iterator(file *fp, file *ifp, blk_size bs, keytype key1, keytype key2);
	blk_addr get_next(vector<char> &buffer);
	~table_iterator();
};

template <class keytype> table_iterator<keytype>::table_iterator(file *fp, file *ifp, blk_size bs):inx(ifp, SEC_INDEX_ORDER){
	tfile = fp;
	inxfile = ifp;
	bsize = bs;
	flag = true;
	init = true;
	start = -1;
	end = MAX_RECORDS;
	rkeys = new keytype[SEC_INDEX_ORDER+1];
	rpointers = new blk_addr[SEC_INDEX_ORDER+1];
	pos = 0;
	num = 0;
}

template <class keytype> table_iterator<keytype>::table_iterator(file *fp, file *ifp, blk_size bs, keytype key1, keytype key2):inx(ifp, SEC_INDEX_ORDER){
	tfile = fp;
	inxfile = ifp;
	bsize = bs;
	flag = true;
	init = true;
	start = key1;
	end = key2;
	rkeys = new keytype[SEC_INDEX_ORDER+1];
	rpointers = new blk_addr[SEC_INDEX_ORDER+1];
	pos = 0;
	num = 0;
}



template <class keytype> blk_addr table_iterator<keytype>::get_next(vector<char> &buffer){
	if(!flag) return -1;

	if(init){
		inx.find_records_range(start, end, rkeys, rpointers, &num);
		init = false;
	}

	if(pos == num){
		if(!inx.find_next()){
			flag = false;
			return -1;
		}
		pos = 0;
	} 

	void *buf = tfile->read_block(rpointers[pos]);
	if(!buf){
		cerr << "System inconsistent\n";
		flag = false;
		return -1;
	}
	buffer.resize(bsize);
	memcpy((void *)(&(buffer[0])), buf, bsize);
	tfile->unread_block(rpointers[pos]);
	pos += 1;
	return rpointers[pos-1];
}

template <class keytype> table_iterator<keytype>::~table_iterator(){
	delete tfile;
	delete inxfile;
	delete rkeys;
	delete rpointers;
}

#endif