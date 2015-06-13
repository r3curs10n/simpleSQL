#ifndef FILE_H
#define FILE_H
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <set>
#include <map>
#include <vector>
#include <queue>
#include <stack>
#include <cassert>
#include <cmath>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>

#define MAX_SIZE INT_MAX / 2			//max size of file (excluding d header) in bytes
#define MAX_FRAMES 4096		//dis limits no. of blocks in main memory

using namespace std;

typedef int blk_addr;
typedef int blk_size;

struct tuple
{
	void *buf;
	bool dirty;
	bool unread;
	tuple() { }
	tuple(void *b, bool d, bool u): buf(b), dirty(d), unread(u) { }
};

typedef map<blk_addr, tuple > mp;
typedef map<blk_addr, tuple >::iterator imp;
typedef set<blk_addr>::iterator iset;

struct info
{
	unsigned char *bitmap;
	blk_size bl_size;
	bool dirty;
	blk_addr used_block;			//no. of blocks (including holes) upto which the file has been extended
};

class file
{
public:
	info header;				//header to store offset , bitmap ,  block_size , used_block
	queue<blk_addr> free_list;		// a list of blocks which will be next allocated . (Implemented such dat holes will b allocated first)
	blk_addr max_block;				// no. of blocks(excluding d header) largest file can have
	blk_addr max_bytes;				// no. of bytes used by bitmap
	int _offset;				//length of offset in bytes, I need to know whenever file opens
	blk_size block_size;				//block_size in bytes
	mp page_tbl;				
	FILE *fp;
	set <blk_addr> dirty_pages;		//stores d list of block numbers which need 2 b written to the file at d time of closing
	set <blk_addr> unread_pages;		//stores d list of block numbers which need 2 b removed when memory is insufficient
	//information about dese pages will also b in page_tbl, dese data structures are only fr quick access

	inline bool exists(const char *fname){		//returns true if file already exists
		struct stat buffer;
		return !(stat(fname, &buffer));
	}
	inline void set_nth_bit(blk_addr idx)			//idex ranges from 0 to MAX_SIZE - 1 
	{
		//cout << idx << "\n";
	    header.bitmap[idx / CHAR_BIT] |= 1 << (idx % CHAR_BIT);
	}

	inline void clear_nth_bit(blk_addr idx)
	{
	    header.bitmap[idx / CHAR_BIT] &= ~(1 << (idx % CHAR_BIT));
	}

	inline int get_nth_bit(blk_addr idx)
	{
	    return (header.bitmap[idx / CHAR_BIT] >> (idx % CHAR_BIT)) & 1;
	}
	int fsize(const char *filename) {										//returns size of file in bytes
	    struct stat st; 				//but dis fn is not reliable probably u'll hav 2 close d file fr changes in size 2 hav taken place

	    if (stat(filename, &st) == 0)
	        return st.st_size;

	    return -1; 
	}
	bool create(const char *fname, blk_size bl_size, int offset);	//returns true on success false on failuure. creates a file of size 10 KB
	bool open(const char *fname);
	bool free_page(char **buf);											//makes space for a new frame in main memory if possible

public:
	bool success;				//denotes whether d object was successfully creadted; made it public so dat can b checked

	file(const char *fname);
	file(const char *fname, blk_size bl_size, int offset);
	~file();
	
	blk_addr allocate_block();										//returns d index of newly allocated block
	void *read_block(blk_addr block_address);						//reads starting address of buffer in main memory containg block's contents

	int write_block(blk_addr block_address, void *buf_addr);		//block at buf_addr (in memory) is written at block_address in dis file
	//dirty bit is reset but d block is still in buffer hence unread is set so dat when needed dis memory can be freed

	void delete_file(const char *fname);						//don't misbehave wid d object after calling dis fn
	int deallocate_block(blk_addr block_address);
	int unread_block(blk_addr block_address);						//just sets d unread bit to true . when in memory crunch, I can frre dis 
	//frame after writing it in file if dirty bit is set. Even after calling unread_block d block might still be in buffer. If u ask 2 read 
	//it I will just give d address of d block (and reset unread) which I maintain in a map, i.e I won't allocate new memory. Since I don't 
	//free d memory after u call unread, it means buffer is still dere, hence u can directly access it wout asking me 2 read it but u shd
	//avoid it. Since in that case I won't be able to reset unread flag hence, I might end freeing that frame and u r unaware of dis

	int write_back(blk_addr block_address);							//just sets the dirty bit and d block is still in buffer
};
#endif
/* I cannot free d buffer which I receive in write_block fn bcoz dat buffer might hav come directly frm upper layer but all d buffer which I 
   hav in my map are allocated by me at d time of reading block hence I can free dem. I will free dem only when dere is not sufficient memory
   I will free only dose frames fr which unread is set . I can't free dose frames fr which dirty bit is false bcoz d usr might hav just
   read it and after dis step he wants 2 write smthing in d block but I freed it. This is wrong.
   Even I won't eactly free d frame in free_page, I will return d starting buffer address of d fram (fr which unread is set) 2 d new block
   dis memry crunch arises in read_block and not in allocate_block since memory-crunch is associated wid main memory
*/
//Note : MAX_SIZE is size excluding header but ftell() gives size including d header