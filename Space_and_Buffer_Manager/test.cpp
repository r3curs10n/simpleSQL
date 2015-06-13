#include "file.h"


file::file(const char *fname)
{
	success = false;
	if (exists(fname)){
		if (open(fname))
			success = true;
	}
}
file::file(const char *fname, blk_size bl_size, int offset = 0)
{
	success = false;
	if (!exists(fname)){
		if (create(fname, bl_size, offset))
			success = true;
	}
}
file::~file()
{
	/* Since in dis program I can only set header's dirty bit, I cannot reset it. And used_block can only increase, hence whenevr used_block
		increases, header's dirty bit is set
	*/
	if (success){

		fseek(fp, _offset + 2 * sizeof(int), SEEK_SET);		//value of _offset, length of _offset, value of block_size
		fwrite(&header.used_block, sizeof(int), 1, fp);	
		if (header.dirty) {
			fwrite(header.bitmap, sizeof(char), max_bytes, fp);
		}
		//implemented write_block using SEEK_CUR, amortized better performance
		for (iset set_iter = dirty_pages.begin(); set_iter != dirty_pages.end(); ++set_iter){	
			write_block(*set_iter, page_tbl[*set_iter].buf);
		}
		delete[] header.bitmap;
		if (fp) fclose(fp);
	}
	fp = NULL;
}
/*
	Order of various things in file :
	1) length of offset (integer)
	2) offset no. bytes
	3) block_size (integer)
	4) no. of used_block (integer)
	5) bitmap (takes max_bytes no. of bytes)

	Header which consists of all the info which I need whenever file is opened : bitmap, block size, no. of used blocks in the file 
	I the structure , there is also a field bool dirty but it is not stored in the file
	the way I have implemented free_list, it first tries to fill holes b4 extending the file
*/
bool file::create(const char *fname, blk_size bl_size, int offset = 0) //bl_size nd offset are in number of bytes
{
	fp = fopen(fname, "wb+");
	if (!fp){
		return false;
	}
	max_block = MAX_SIZE / bl_size;		//maximum no. of blocks since each block corresponds to 1 bit max_block no. of bit
	max_bytes = max_block / CHAR_BIT;
	int size = max_bytes + sizeof(int) + sizeof(blk_addr) + sizeof(blk_size) + offset;  
																		//size bytes are used up in the file for writing preliminary info
	fseek(fp, size - 1, SEEK_SET);
	char c = '\0';
	fwrite(&c, sizeof(char), 1, fp);
	//till here file of size = size(header is created)

	unsigned char *bitmap = new unsigned char[max_bytes]; 	//one char corresponds to 8 bits
	fill (bitmap, bitmap + max_bytes, 0);

	header.dirty = false;
	header.bitmap = bitmap;
	header.bl_size = bl_size;
	header.used_block = 0;

	block_size = bl_size;
	_offset = offset;

	fseek(fp, 0, SEEK_SET);	
	fwrite(&_offset, sizeof (int), 1, fp);				//writes length off _offset (int)
	
	fseek(fp, _offset, SEEK_CUR);						//actual _offset
	
	fwrite(&header.bl_size, sizeof (blk_size), 1, fp);	//writes block_size
	
	fwrite(&header.used_block, sizeof (blk_addr), 1, fp);	//writes no. of used_block (int)
	
	fwrite(header.bitmap, sizeof (char), max_bytes , fp);	//writes d bitmap

	free_list.push(header.used_block);
	return true;
	//fp now points to position after header
}
bool file::open(const char *fname)
{
	fp = fopen(fname, "rb+");
	if (!fp){
		return false;
	}
	
	fread(&_offset, sizeof (int), 1, fp);
	
	fseek(fp, _offset, SEEK_CUR);
	
	fread(&header.bl_size, sizeof (blk_size), 1, fp);
	
	fread(&header.used_block, sizeof (blk_addr), 1, fp);

	max_block = MAX_SIZE / header.bl_size;		//maximum no. of blocks since each block corresponds to 1 bit max_block no. of bit
	max_bytes = max_block / CHAR_BIT;
	header.bitmap =  new unsigned char[max_bytes];

	fread(header.bitmap, sizeof(char), max_bytes, fp);		//initializing header
	
	header.dirty = false;
	block_size = header.bl_size;

	for (blk_addr i = 0; i < header.used_block; ++i){
		int d = get_nth_bit(i);
		if (!d) free_list.push(i);
	}
	free_list.push(header.used_block);
	//free_list is guaranteed 2 conatin atleast 1 block
	
	return true;
	//fp now oints to position after header
}
int file::unread_block(blk_addr block_address)
{
	if (page_tbl.count(block_address) <= 0) {
		return -2;
	}
	page_tbl[block_address].unread = true;
	unread_pages.insert(block_address);
	return 2;
}
int file::write_back(blk_addr block_address)
{
	if (page_tbl.count(block_address) <= 0) {
		return -2;
	}
	page_tbl[block_address].dirty = true;
	dirty_pages.insert(block_address);
	return 2;
}
int file::write_block(blk_addr block_address,void *buf)		//dis fn only writes d block d calling fn shd take care of setting/ resetting d flags
{	//since dis is a generic fn called even if dis is a new block or was read earlier
	if (!get_nth_bit(block_address)){
		return -3;
	}
	fseek(fp, _offset + max_bytes + sizeof(int) + sizeof(blk_addr) + sizeof(blk_size) + block_size * block_address - ftell(fp), SEEK_CUR);
	//FILE *temp
	int w = fwrite(buf, sizeof(char), block_size, fp);
	if (w < block_size) {
		deallocate_block(block_address);
		return -4;
	}
	else{
		return 2;	
	} 
}
/*
i cannot remove non-dirty buffer as long as user has read it bcoz user might later expect 2 modify dat buffer but if he has unread it,
it means if he needs it again, he will read it agin
*/
bool file::free_page(char **buffer)
{
	for (iset set_iter = unread_pages.begin(); set_iter != unread_pages.end(); ++set_iter){	
		*buffer = (char *)page_tbl[*set_iter].buf;
		if (page_tbl[*set_iter].dirty){
			page_tbl[*set_iter].dirty = false;
			dirty_pages.erase(*set_iter);
			write_block(*set_iter, *buffer);	
		}
		page_tbl.erase(*set_iter);
		return true;
	}
	return false;
}
void *file::read_block(blk_addr block_address)		//block_address is zero based indexed		returns a char buffer
{
	if (!get_nth_bit(block_address)){
		return NULL;
	}
	if (page_tbl.count(block_address) > 0) {

		if (page_tbl[block_address].unread) {		//could hav used erase wout checking but dis is more optimal
			unread_pages.erase(block_address);			
		}

		page_tbl[block_address].unread = false;		//since u hav read d bock, u might wanna perform sm operation, hence I shd not remove it
		return page_tbl[block_address].buf;
	}
	char *buf;
	if (page_tbl.size() >= MAX_FRAMES) {							
		if (!free_page(&buf)) return NULL;
	}
	else {
		buf = new char[block_size];
		if (!buf){
			return NULL;
		}
	}
	
	fseek(fp,  _offset + max_bytes + sizeof(int) + sizeof(blk_addr) + sizeof(blk_size) + block_size * block_address - ftell(fp), SEEK_CUR);
	
	fread(buf, sizeof(char), block_size, fp);
	page_tbl[block_address] = tuple(buf, false, false);
	return buf;
}
void file::delete_file(const char *fname)
{
	////reset variables of object
	fclose(fp);
	fp = NULL;
	string n = "rm ";
	n += string(fname);
	system(n.c_str());
}
int file::deallocate_block(blk_addr block_address)
{
	if (block_address >= max_block){
		return -5;
	}
	if (!get_nth_bit(block_address)){
		return -3;
	}
	clear_nth_bit(block_address);
	header.dirty = true;
	free_list.push(block_address);
	page_tbl.erase(block_address);
	dirty_pages.erase(block_address);
	unread_pages.erase(block_address);
	return 2;
}
blk_addr file::allocate_block()		//append a memory chunk equal to bl_size
{
	blk_addr i = free_list.front();		//deciding which block 2 allocate
	if (i < max_block)	free_list.pop();
	//free_list conatins header.used_block initially, hence it is either dere in free_list or if it is not dere => it was allocated 2 some blk
	//hence if free_list.size() = 0 => used_block is already allocated hence push used_block + 1
	if (free_list.size() == 0) { free_list.push(header.used_block + 1); }
	
	if (i < header.used_block) {					//some block in d file is now free
		set_nth_bit(i);
		header.dirty = true;
		return i;
	}
	//now I will need 2 append a block 2 d file
	if (header.used_block >= max_block){	
		return -6;
	}

	fseek(fp, 0, SEEK_END);
	
	fseek(fp, block_size - 1, SEEK_END);
	char c = '\0';
	fwrite(&c, sizeof(char), 1, fp);
	
	header.dirty = true;
	set_nth_bit(header.used_block);
	
	++header.used_block;
	return i ;
}
int main()
{
	file f("test/a.bin");	
	cout << "Hi\n";
	int index;
	char **c = new char *[5];
	c[0] = (char *)f.read_block(1);
	if (c[0] == NULL) cout << "Read failed\n";
	for (int i = 0; i < 2; i++){
		index = f.allocate_block();
		if (index < 0) {
			cout << "Block could not get allocated\n";
			break;
		}
		cout << index << "\n";
		cout << "Block allocated : " << index << "\n";
		c[i] = (char *)f.read_block(index);
	}
	c[4] = (char *)f.read_block(1);
	f.unread_block(1);
	for (int i = 2; i < 4; i++){
		index = f.allocate_block();
		if (index < 0) {
			cout << "Block could not get allocated\n";
			break;
		}
		cout << index << "\n";
		cout << "Block allocated : " << index << "\n";
		c[i] = (char *)f.read_block(index);
	}
	c[4] = (char *)f.read_block(4);
	*c[4] = 'd';
	f.write_back(4);
	c[4] = (char *)f.read_block(4);
	/*char buf[1024];
	for (int i = 0; i < 1024; ++i)
	{
		buf[i] = '2';
	}
	f.write_block(index, (void *)buf);
	char *s = (char *)f.read_block(index);
	for (int i = 0; i < 1024; ++i){
		cout << s[i] << "\n";
	}*/
	//f.create();
	//f.write_block()
	return 0;
}