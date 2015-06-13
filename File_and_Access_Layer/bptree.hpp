//B+ Tree implementation
#ifndef BPTREE_H
#define BPTREE_H

#include "../Space_and_Buffer_Manager/file.h"

using namespace std;

#define DEFAULT_ORDER 4
#define MIN_ORDER 3
#define MAX_ORDER 20

template <class keytype> struct bnode{
	blk_addr *pointers;
	keytype *keys;
	blk_addr parent;
	bool is_leaf;
	int num_keys;
	blk_addr baddr;		//blk addr in the disk
	char *block;		//block in the disk
	struct bnode *next;	//Used for queue.
};

template <class keytype> class bptree{

	file *indexfile;
	int keylen;
	int addrlen;
	int order;
	bnode<keytype> *qu;
	bnode<keytype> *root;
	map<blk_addr, bnode<keytype> *> actblks; //active blocks
	vector<bnode<keytype> *> newblks; //newly created blocks
	keytype *returned_keys;
	blk_addr *returned_pointers;
	int *numkeys;
	keytype key_start;
	keytype key_end;
	bnode<keytype> *next;

	// Output and utility.
	bnode<keytype> *read_node(blk_addr baddr);
	void write_node(bnode<keytype> *n);
	void enqueue(bnode<keytype> *new_node);
	bnode<keytype> *dequeue();
	int height();
	int path_to_root(bnode<keytype> *child); 
	bnode<keytype> *find_leaf(keytype key);
	blk_addr find(keytype key);
	int cut(int length);
	void freemem(bool flag);
	void update_rblk(blk_addr rblk);

	// Insertion.
	bnode<keytype> *make_node();
	bnode<keytype> *make_leaf();
	int get_left_index(bnode<keytype> *parent, bnode<keytype> *left);
	bnode<keytype> *insert_into_leaf(bnode<keytype> *leaf, keytype key, blk_addr recordid);
	bnode<keytype> *insert_into_leaf_after_splitting(bnode<keytype> *leaf, keytype key, blk_addr recordid);
	bnode<keytype> *insert_into_node(bnode<keytype> *n, int left_index, 
			keytype key, bnode<keytype> *right);
	bnode<keytype> *insert_into_node_after_splitting(bnode<keytype> *old_node, int left_index, 
			keytype key, bnode<keytype> *right);
	bnode<keytype> *insert_into_parent(bnode<keytype> *left, keytype key, bnode<keytype> *right);
	bnode<keytype> *insert_into_new_root(bnode<keytype> *left, keytype key, bnode<keytype> *right);
	bnode<keytype> *start_new_tree(keytype key, blk_addr recordid);

	// Deletion.
	int get_neighbor_index(bnode<keytype> *n);
	bnode<keytype> *remove_entry_from_node(bnode<keytype> *n, keytype key, blk_addr pointer);
	bnode<keytype> *adjust_root();
	bnode<keytype> *coalesce_nodes(bnode<keytype> *n, bnode<keytype> *neighbor, int neighbor_index, keytype k_prime); //chk
	bnode<keytype> *redistribute_nodes(bnode<keytype> *n, bnode<keytype> *neighbor, int neighbor_index, //chk
			int k_prime_index, keytype k_prime);
	bnode<keytype> *delete_entry(bnode<keytype> *n, keytype key, blk_addr pointer); //chk


public:
	bptree(file *ifile);
	bptree(file *ifile, int od);
	//~bptree();

	// Output and utility.
	void print_leaves();
	//void print_tree();
	void print_node(bnode<keytype> *n);
	blk_addr find_record(keytype key); 
	void find_records_range(keytype key1, keytype key2, 
					keytype *rkeys, blk_addr *rpointers, int *num);
	bool find_next();

	// Insertion
	bnode<keytype> *insert(keytype key, blk_addr recordid);

	//Deletion
	bnode<keytype> *remove(keytype key);
	

	void check();

};

template <class keytype> bptree<keytype>::bptree(file *ifile){
	indexfile = ifile;
	keylen = sizeof(keytype);
	addrlen = sizeof(blk_addr);
	order = DEFAULT_ORDER;
	qu = NULL;
	actblks = map<blk_addr, bnode<keytype> *>();
	newblks = vector<bnode<keytype> *>();
	returned_keys = NULL;
	returned_pointers = NULL;
	numkeys = NULL;
	next = NULL;

	blk_addr rblk;
	void *buf = indexfile->read_block(0);
	rblk = *((blk_addr *)buf);
	indexfile->unread_block(0);
	if(rblk == -1) root = NULL;
	else root = read_node(rblk);
}

template <class keytype> bptree<keytype>::bptree(file *ifile, int od){
	indexfile = ifile;
	keylen = sizeof(keytype);
	addrlen = sizeof(blk_addr);
	order = od;
	qu = NULL;
	actblks = map<blk_addr, bnode<keytype> *>();
	newblks = vector<bnode<keytype> *>();
	returned_keys = NULL;
	returned_pointers = NULL;
	numkeys = NULL;
	next = NULL;

	blk_addr rblk;
	void *buf = indexfile->read_block(0);
	rblk = *((blk_addr *)buf);
	indexfile->unread_block(0);
	if(rblk == -1) root = NULL;
	else root = read_node(rblk);
}


// Output and utility.
template <class keytype> bnode<keytype> *bptree<keytype>::read_node(blk_addr baddr){
	if(baddr < 0) return NULL;

	if(actblks.find(baddr) == actblks.end()){
		char *block = (char *)(indexfile->read_block(baddr));
		if(!block) return NULL;

		bnode<keytype> *n = new bnode<keytype>;
		if(!n){
			indexfile->unread_block(baddr);
			return NULL;
		}
		n->baddr = baddr;
		n->block = block;
		n->is_leaf = *((int *)block);
		n->num_keys = *((int *)(block+sizeof(int)));
		n->parent = *((blk_addr *)(block+2*sizeof(int)));
		n->pointers = (blk_addr *)(block+2*sizeof(int)+addrlen);
		n->keys = (keytype *)(block+2*sizeof(int)+(order+1)*addrlen);
		n->next = NULL;
		actblks[baddr] = n;
		return n;
	}
	else return actblks[baddr];
}

template <class keytype> void bptree<keytype>::write_node(bnode<keytype> *n){
	if(!n) return;
	char *block = n->block;
	*((int *)block) = n->is_leaf;
	*((int *)(block+sizeof(int))) = n->num_keys;
	*((blk_addr *)(block+2*sizeof(int))) = n->parent;
	indexfile->write_back(n->baddr);
}

template <class keytype> int bptree<keytype>::cut(int length){
	if(length % 2 == 0)
		return length/2;
	else
		return length/2 + 1;
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
template <class keytype> void bptree<keytype>::enqueue(bnode<keytype> *new_node){
	bnode<keytype> *c;
	if(qu == NULL){
		qu = new_node;
		qu->next = NULL;
	}
	else{
		c = qu;
		while(c->next != NULL){
			c = c->next;
		}
		c->next = new_node;
		new_node->next = NULL;
	}
}


/* Helper function for printing the
 * tree out.  See print_tree.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::dequeue(){
	bnode<keytype> *n = qu;
	qu = qu->next;
	n->next = NULL;
	return n;
}

/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
template <class keytype> void bptree<keytype>::print_leaves(){
	int i;
	bnode<keytype> *c = root;
	if(root == NULL){
		printf("Empty tree.\n");
		return;
	}
	while(!c->is_leaf){
		if(!(c = read_node(c->pointers[0]))){
			cerr << "Error\n";
			freemem(false);
			return;
		}
	}
	while(true){
		for(i = 0; i < c->num_keys; i++){
			cout << (c->pointers)[i] << " " << (c->keys)[i] << " ";
		}
		cout << (c->pointers)[order-1] << endl;
		if(c->pointers[order - 1] != -1){
			printf(" | ");
			if(!(c = read_node(c->pointers[order - 1]))){
				cerr << "Error\n";
				freemem(false);
				return;
			}
		}
		else
			break;
	}
	freemem(false);
	printf("\n");
}


/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
template <class keytype> int bptree<keytype>::height(){
	int h = 0;
	bnode<keytype> *c = root;
	while(!c->is_leaf){
		if(!(c = read_node(c->pointers[0]))){
			return -1;
		}
		h++;
	}
	return h;
}

/* Utility function to give the length in edges
 * of the path from any node<keytype> to the root.
 */
template <class keytype> int bptree<keytype>::path_to_root(bnode<keytype> *child){
	int length = 0;
	bnode<keytype> *c = child;
	while(c != root){
		if(!(c = read_node(c->parent))){
			return -1;
		}
		length++;
	}
	return length;
}

/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
template <class keytype> blk_addr bptree<keytype>::find_record(keytype key){
	blk_addr r = find(key);
	freemem(false);
	return r; 
}

/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
template <class keytype> void bptree<keytype>::find_records_range(keytype key1, keytype key2, 
					keytype *rkeys, blk_addr *rpointers, int *num){
	key_start = key1;
	key_end = key2;
	returned_keys = rkeys;
	returned_pointers = rpointers;
	numkeys = num;
	next = find_leaf(key_start);
}


/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
template <class keytype> bool bptree<keytype>::find_next(){
	//node *n = find_leaf(key_start);
	bool flag = true;
	if(next == NULL) flag = false;
	else{
		int i = 0;
		*numkeys = 0;
		for(i = 0; i < next->num_keys && next->keys[i] < key_start; i++) ;
		if(i != next->num_keys){
			for( ; i < next->num_keys && next->keys[i] <= key_end; i++){
				returned_keys[*numkeys] = next->keys[i];
				returned_pointers[*numkeys] = next->pointers[i];
				(*numkeys)++;
			}
		}
		if(*numkeys == 0) flag = false;
	}
	if(!flag){
		returned_keys = NULL;
		returned_pointers = NULL;
		numkeys = NULL;
		next = NULL;
		freemem(false);
		return false;
	}
	else{
		blk_addr temp = next->pointers[order-1];
		freemem(false);
		next = read_node(temp);
		return true;
	}
}


/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::find_leaf(keytype key){
	int i = 0;
	bnode<keytype> *c = root;
	if(c == NULL){
		return c;
	}
	while(!c->is_leaf){
		i = 0;
		while(i < c->num_keys){
			if(key >= c->keys[i]) i++;
			else break;
		}
		if(!(c = read_node(c->pointers[i])))
			return NULL;
	}
	return c;
}


/* Finds and returns the record to which
 * a key refers.
 */
template <class keytype> blk_addr bptree<keytype>::find(keytype key){
	int i = 0;
	bnode<keytype> *c = find_leaf(key);
	if(c == NULL) return -1;
	for(i = 0; i < c->num_keys; i++)
		if(c->keys[i] == key) break;
	if(i == c->num_keys) 
		return -1;
	else
		return c->pointers[i];
}

template <class keytype> void bptree<keytype>::print_node(bnode<keytype> *n){
	cout << "The block address of the node in the disk is " << n->baddr << endl;
	if(n->is_leaf) cout << "This is a leaf\n";
	else cout << "This is not a leaf\n";
	cout << "The number of keys is/are " << n->num_keys << endl;
	cout << "The keys and pointers are as:\n";
	cout << (n->pointers)[0] << endl;
	for(int i = 0; i < n->num_keys; i++){
		cout << (n->keys)[i] << " " << (n->pointers)[i+1] << endl;
	}
	cout << "_________________________________________________\n";
	return;
}

template <class keytype> void bptree<keytype>::check(){
	bnode<keytype> *new_node;
	int temp;
	new_node = make_node();
	print_node(new_node);
	for(int i = 0; i < 14; i++){
		(new_node->keys)[i] = 100*i;
		(new_node->pointers)[i] = 100 - i;
	}
	(new_node->pointers)[14] = -1;
	new_node->num_keys = 14;
	temp = new_node->baddr;
	freemem(true);

	bnode<keytype> *n = read_node(temp);
	print_node(n);
	freemem(false);
}

template <class keytype> void bptree<keytype>::freemem(bool flag){
	typename map<blk_addr, bnode<keytype> *>::iterator it;
	it = actblks.begin();
	
	while(it != actblks.end()){
		if(flag) write_node(it->second);
		if(it->second != root){
   			typename map<blk_addr, bnode<keytype> *>::iterator toErase = it;
   			++it;
   			indexfile->unread_block(toErase->first);
   			delete toErase->second;
   			actblks.erase(toErase);
		} 
		else ++it;
	}
}



//INSERTION

/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::make_node(){
	blk_addr baddr = indexfile->allocate_block();
	if(baddr < 0) return NULL;

	char *block = (char *)(indexfile->read_block(baddr));
	if(!block){
		indexfile->deallocate_block(baddr);
	}
	bnode<keytype> *new_node = new bnode<keytype>;
	if(!new_node){
		indexfile->unread_block(baddr);
		indexfile->deallocate_block(baddr);
		return NULL;
	}

	new_node->baddr = baddr;
	new_node->block = block;
	new_node->pointers = (blk_addr *)(block+2*sizeof(int)+addrlen);
	new_node->keys = (keytype *)(block+2*sizeof(int)+(order+1)*addrlen);
	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent = -1;
	new_node->next = NULL;
	newblks.push_back(new_node);
	actblks[baddr] = new_node;
	return new_node;
}

/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::make_leaf(){
	bnode<keytype> *leaf = make_node();
	if(leaf) leaf->is_leaf = true;
	return leaf;
}



/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
template <class keytype> int bptree<keytype>::get_left_index(bnode<keytype> *parent, bnode<keytype> *left){

	int left_index = 0;
	while(left_index <= parent->num_keys && 
			parent->pointers[left_index] != left->baddr)
		left_index++;
	return left_index;
}

/* Inserts a recordid and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::insert_into_leaf(bnode<keytype> *leaf, keytype key, blk_addr recordid){

	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
		insertion_point++;

	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pointers[i] = leaf->pointers[i - 1];
	}
	leaf->keys[insertion_point] = key;
	leaf->pointers[insertion_point] = recordid;
	leaf->num_keys++;
	return leaf;
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::insert_into_leaf_after_splitting(bnode<keytype> *leaf, keytype key, blk_addr recordid){

	bnode<keytype> *new_leaf;
	keytype *temp_keys;
	blk_addr *temp_pointers;
	int insertion_index, split, i, j;
	keytype new_key;

	if(!(new_leaf = make_leaf())) return NULL;

	temp_keys = new keytype[order];
	if(!temp_keys) return NULL;

	temp_pointers = new blk_addr[order];
	if(!temp_pointers){
		delete[] temp_keys;
		return NULL;
	}

	insertion_index = 0;
	while(insertion_index < order - 1 && leaf->keys[insertion_index] < key)
		insertion_index++;

	for(i = 0, j = 0; i < leaf->num_keys; i++, j++){
		if(j == insertion_index) j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->pointers[i];
	}

	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = recordid;

	leaf->num_keys = 0;

	split = cut(order - 1);

	for(i = 0; i < split; i++){
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	for(i = split, j = 0; i < order; i++, j++){
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}

	delete[] temp_pointers;
	delete[] temp_keys;

	new_leaf->pointers[order - 1] = leaf->pointers[order - 1];
	leaf->pointers[order - 1] = new_leaf->baddr;

	for (i = leaf->num_keys; i < order - 1; i++)
		leaf->pointers[i] = -1;
	for (i = new_leaf->num_keys; i < order - 1; i++)
		new_leaf->pointers[i] = -1;

	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];

	return insert_into_parent(leaf, new_key, new_leaf);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::insert_into_node(bnode<keytype> *n, 
		int left_index, keytype key, bnode<keytype> *right){
	int i;
	for(i = n->num_keys; i > left_index; i--){
		n->pointers[i + 1] = n->pointers[i];
		n->keys[i] = n->keys[i - 1];
	}
	n->pointers[left_index + 1] = right->baddr;
	n->keys[left_index] = key;
	n->num_keys++;
	return root;
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::insert_into_node_after_splitting(bnode<keytype> *old_node, int left_index, 
		keytype key, bnode<keytype> *right){

	int i, j, split;
	keytype k_prime;
	bnode<keytype> *new_node, *child;
	keytype *temp_keys;
	blk_addr *temp_pointers;

	/* First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */

	temp_pointers = new blk_addr[order+1];
	if(temp_pointers == NULL) return NULL;

	temp_keys = new keytype[order];
	if(temp_keys == NULL){
		delete[] temp_pointers;
		return NULL;
	}

	for(i = 0, j = 0; i < old_node->num_keys + 1; i++, j++){
		if(j == left_index + 1) j++;
		temp_pointers[j] = old_node->pointers[i];
	}

	for(i = 0, j = 0; i < old_node->num_keys; i++, j++){
		if(j == left_index) j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right->baddr;
	temp_keys[left_index] = key;

	/* Create the new node and copy
	 * half the keys and pointers to the
	 * old and half to the new.
	 */  
	split = cut(order);
	if(!(new_node = make_node())){
		delete[] temp_pointers;
		delete[] temp_keys;
		return NULL;
	}
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	k_prime = temp_keys[split - 1];
	for (++i, j = 0; i < order; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->num_keys++;
	}
	new_node->pointers[j] = temp_pointers[i];
	delete[] temp_pointers;
	delete[] temp_keys;
	new_node->parent = old_node->parent;
	for(i = 0; i <= new_node->num_keys; i++){
		child = read_node(new_node->pointers[i]);
		child->parent = new_node->baddr;
	}

	/* Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old node to the left and the new to the right.
	 */

	return insert_into_parent(old_node, k_prime, new_node);
}



/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::insert_into_parent(bnode<keytype> *left, keytype key, bnode<keytype> *right){

	int left_index;
	bnode<keytype> *parent;

	parent = read_node(left->parent);

	/* Case: new root. */

	if(parent == NULL)
		return insert_into_new_root(left, key, right);

	/* Case: leaf or node. (Remainder of
	 * function body.)  
	 */

	/* Find the parent's pointer to the left 
	 * node.
	 */

	left_index = get_left_index(parent, left);


	/* Simple case: the new key fits into the node. 
	 */

	if(parent->num_keys < order - 1)
		return insert_into_node(parent, left_index, key, right);

	/* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */

	return insert_into_node_after_splitting(parent, left_index, key, right);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::insert_into_new_root(bnode<keytype> *left, keytype key, bnode<keytype> *right){

	bnode<keytype> *temp;
	if(!(temp = make_node())) return NULL;
	root = temp;
	root->keys[0] = key;
	root->pointers[0] = left->baddr;
	root->pointers[1] = right->baddr;
	root->num_keys++;
	root->parent = -1;
	left->parent = root->baddr;
	right->parent = root->baddr;
	return root;
}



/* First insertion:
 * start a new tree.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::start_new_tree(keytype key, blk_addr recordid){

	if(!(root = make_leaf())) return NULL;
	root->keys[0] = key;
	root->pointers[0] = recordid;
	root->pointers[order - 1] = -1;
	root->parent = -1;
	root->num_keys++;
	return root;
}


template <class keytype> void bptree<keytype>::update_rblk(blk_addr rblk){
	void *buf = indexfile->read_block(0);
	*((blk_addr *)buf) = rblk;
	indexfile->write_back(0);
	indexfile->unread_block(0);
	//cout << "The root block is: " << rblk << endl;
}


/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::insert(keytype key, blk_addr recordid){

	bnode<keytype> *leaf;
	bnode<keytype> *temp;
	
	/* The current implementation ignores
	 * duplicates.
	 */

	if(find(key) != -1){
		freemem(false);
		return NULL;
	}

	/* Case: the tree does not exist yet.
	 * Start a new tree.
	 */

	if(root == NULL){
		temp = start_new_tree(key, recordid);
		freemem(true);
		newblks.clear();
		update_rblk(temp->baddr);
		return temp;
	}


	/* Case: the tree already exists.
	 * (Rest of function body.)
	 */

	leaf = find_leaf(key);
	if(!leaf){
		freemem(false);
		return NULL;
	}

	/* Case: leaf has room for key and recordid.
	 */

	if(leaf->num_keys < order - 1){
		leaf = insert_into_leaf(leaf, key, recordid);
		freemem(true);
		return root;
	}


	/* Case:  leaf must be split.
	 */

	temp = insert_into_leaf_after_splitting(leaf, key, recordid);
	if(!temp){
		freemem(false);
		while(!newblks.empty()){
			indexfile->deallocate_block((newblks.back())->baddr);
			newblks.pop_back();
		}
		return NULL;
	}
	else{
		freemem(true);
		newblks.clear();
		update_rblk(temp->baddr);
		return temp;
	}
}

//Deletion

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
template <class keytype> int bptree<keytype>::get_neighbor_index(bnode<keytype> *n){

	int i;
	bnode<keytype> *c;
	/* Return the index of the key to the left
	 * of the pointer in the parent pointing
	 * to n.  
	 * If n is the leftmost child, this means
	 * return -1.
	 */
	if(!(c = read_node(n->parent))){
		return -2;
	}
	for(i = 0; i <= c->num_keys; i++)
		if((c->pointers)[i] == n->baddr)
			return i - 1;

	return -2;
}

template <class keytype> bnode<keytype> *bptree<keytype>::remove_entry_from_node(bnode<keytype> *n, keytype key, blk_addr pointer){

	int i, num_pointers;

	// Remove the key and shift other keys accordingly.
	i = 0;
	while (n->keys[i] != key)
		i++;
	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

	// Remove the pointer and shift other pointers accordingly.
	// First determine number of pointers.
	num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
	i = 0;
	while(n->pointers[i] != pointer)
		i++;
	for(++i; i < num_pointers; i++)
		n->pointers[i - 1] = n->pointers[i];


	// One key fewer.
	n->num_keys--;

	// Set the other pointers to NULL for tidiness.
	// A leaf uses the last pointer to point to the next leaf.
	if (n->is_leaf)
		for (i = n->num_keys; i < order - 1; i++)
			n->pointers[i] = -1;
	else
		for (i = n->num_keys + 1; i < order; i++)
			n->pointers[i] = -1;

	return n;
}


template <class keytype> bnode<keytype> *bptree<keytype>::adjust_root(){

	bnode<keytype> *new_root;

	/* Case: nonempty root.
	 * Key and pointer have already been deleted,
	 * so nothing to be done.
	 */

	if(root->num_keys > 0)
		return root;

	/* Case: empty root. 
	 */

	// If it has a child, promote 
	// the first (only) child
	// as the new root.

	if(!root->is_leaf){
		if(!(new_root = read_node(root->pointers[0])))
			return NULL;
		new_root->parent = -1;
	}

	// If it is a leaf (has no children),
	// then the whole tree is empty.

	else{
		new_root = NULL;
		update_rblk(-1);
	}

	indexfile->unread_block(root->baddr);
	indexfile->deallocate_block(root->baddr);
	actblks.erase(root->baddr);
	delete root;

	root = new_root;
	return new_root;
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::coalesce_nodes(bnode<keytype> *n, bnode<keytype> *neighbor, int neighbor_index, keytype k_prime) {

	int i, j, neighbor_insertion_index, n_start, n_end;
	keytype new_k_prime;
	bnode<keytype> *tmp;
	bool split;

	/* Swap neighbor with node if node is on the
	 * extreme left and neighbor is to its right.
	 */

	if(neighbor_index == -1){
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	/* Starting point in the neighbor for copying
	 * keys and pointers from n.
	 * Recall that n and neighbor have swapped places
	 * in the special case of n being a leftmost child.
	 */

	neighbor_insertion_index = neighbor->num_keys;

	/*
	 * Nonleaf nodes may sometimes need to remain split,
	 * if the insertion of k_prime would cause the resulting
	 * single coalesced node to exceed the limit order - 1.
	 * The variable split is always false for leaf nodes
	 * and only sometimes set to true for nonleaf nodes.
	 */

	split = false;

	/* Case:  nonleaf node.
	 * Append k_prime and the following pointer.
	 * If there is room in the neighbor, append
	 * all pointers and keys from the neighbor.
	 * Otherwise, append only cut(order) - 2 keys and
	 * cut(order) - 1 pointers.
	 */

	if (!n->is_leaf) {

		/* Append k_prime.
		 */

		neighbor->keys[neighbor_insertion_index] = k_prime;
		neighbor->num_keys++;


		/* Case (default):  there is room for all of n's keys and pointers
		 * in the neighbor after appending k_prime.
		 */

		n_end = n->num_keys;

		/* Case (special): k cannot fit with all the other keys and pointers
		 * into one coalesced node.
		 */
		n_start = 0; // Only used in this special case.
		if (n->num_keys + neighbor->num_keys >= order) {
			split = true;
			n_end = cut(order) - 2;
		}

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
			n->num_keys--;
			n_start++;
		}

		/* The number of pointers is always
		 * one more than the number of keys.
		 */

		neighbor->pointers[i] = n->pointers[j];

		/* If the nodes are still split, remove the first key from
		 * n.
		 */
		if (split) {
			new_k_prime = n->keys[n_start];
			for (i = 0, j = n_start + 1; i < n->num_keys; i++, j++) {
				n->keys[i] = n->keys[j];
				n->pointers[i] = n->pointers[j];
			}
			n->pointers[i] = n->pointers[j];
			n->num_keys--;
		}

		/* All children must now point up to the same parent.
		 */

		for (i = 0; i < neighbor->num_keys + 1; i++) {
			if(!(tmp = read_node(neighbor->pointers[i]))){
				return NULL;
			}
			tmp->parent = neighbor->baddr;
		}
	}

	/* In a leaf, append the keys and pointers of
	 * n to the neighbor.
	 * Set the neighbor's last pointer to point to
	 * what had been n's right neighbor.
	 */

	else {
		for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
		}
		neighbor->pointers[order - 1] = n->pointers[order - 1];
	}

	bnode<keytype> *np;
	if(!(np = read_node(n->parent))) return NULL;
	if (!split){
		root = delete_entry(np, k_prime, n->baddr);
		indexfile->unread_block(n->baddr);
		indexfile->deallocate_block(n->baddr);
		if(actblks.find(n->baddr) != actblks.end())
			actblks.erase(n->baddr);
		delete n;

	}
	else
		for(i = 0; i < np->num_keys; i++)
			if((np->pointers)[i + 1] == n->baddr){
				(np->keys)[i] = new_k_prime;
				break;
			}

	return root;
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
template <class keytype> bnode<keytype> *bptree<keytype>::redistribute_nodes(bnode<keytype> *n, bnode<keytype> *neighbor, int neighbor_index, 
		int k_prime_index, keytype k_prime){  
	int i;
	bnode<keytype> *tmp;
	bnode<keytype> *np;
	/* Case: n has a neighbor to the left. 
	 * Pull the neighbor's last key-pointer pair over
	 * from the neighbor's right end to n's left end.
	 */

	if(!(np = read_node(n->parent))) return NULL;
	if (neighbor_index != -1) {
		if(!n->is_leaf)
			n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
		for(i = n->num_keys; i > 0; i--){
			n->keys[i] = n->keys[i - 1];
			n->pointers[i] = n->pointers[i - 1];
		}
		if(!n->is_leaf){
			n->pointers[0] = neighbor->pointers[neighbor->num_keys];
			if(!(tmp = read_node(n->pointers[0]))) return NULL;
			tmp->parent = n->baddr;
			neighbor->pointers[neighbor->num_keys] = -1;
			n->keys[0] = k_prime;
			np->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
		}
		else{
			n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
			neighbor->pointers[neighbor->num_keys - 1] = -1;
			n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
			np->keys[k_prime_index] = n->keys[0];
		}
	}

	/* Case: n is the leftmost child.
	 * Take a key-pointer pair from the neighbor to the right.
	 * Move the neighbor's leftmost key-pointer pair
	 * to n's rightmost position.
	 */

	else {  
		if(n->is_leaf){
			n->keys[n->num_keys] = neighbor->keys[0];
			n->pointers[n->num_keys] = neighbor->pointers[0];
			np->keys[k_prime_index] = neighbor->keys[1];
		}
		else{
			n->keys[n->num_keys] = k_prime;
			n->pointers[n->num_keys + 1] = neighbor->pointers[0];
			if(!(tmp = read_node(n->pointers[n->num_keys + 1])))
				return NULL;
			tmp->parent = n->baddr;
			np->keys[k_prime_index] = neighbor->keys[0];
		}
		for (i = 0; i < neighbor->num_keys; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->pointers[i] = neighbor->pointers[i + 1];
		}
		if (!n->is_leaf)
			neighbor->pointers[i] = neighbor->pointers[i + 1];
	}

	/* n now has one more key and one more pointer;
	 * the neighbor has one fewer of each.
	 */

	n->num_keys++;
	neighbor->num_keys--;

	return root;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
template <class keytype> bnode<keytype> *bptree<keytype>::delete_entry(bnode<keytype> *n, keytype key, blk_addr pointer){

	int min_keys;
	bnode<keytype> *neighbor;
	int neighbor_index, k_prime_index;
	keytype k_prime;
	int capacity;

	// Remove key and pointer from node.

	n = remove_entry_from_node(n, key, pointer);

	/* Case:  deletion from the root. 
	 */

	if(n == root) 
		return adjust_root();


	/* Case:  deletion from a node below the root.
	 * (Rest of function body.)
	 */

	/* Determine minimum allowable size of node,
	 * to be preserved after deletion.
	 */

	min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

	/* Case:  node stays at or above minimum.
	 * (The simple case.)
	 */

	if (n->num_keys >= min_keys)
		return root;

	/* Case:  node falls below minimum.
	 * Either coalescence or redistribution
	 * is needed.
	 */

	/* Find the appropriate neighbor node with which
	 * to coalesce.
	 * Also find the key (k_prime) in the parent
	 * between the pointer to node n and the pointer
	 * to the neighbor.
	 */

	bnode<keytype> *np;
	if(!(np = read_node(n->parent))) return NULL;
	if((neighbor_index = get_neighbor_index(n)) == -2) return NULL;
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = np->keys[k_prime_index];
	neighbor = neighbor_index == -1 ? read_node(np->pointers[1]) : 
		read_node(np->pointers[neighbor_index]);
	if(!neighbor) return NULL;
	capacity = n->is_leaf ? order : order - 1;

	/* Coalescence. */

	if (neighbor->num_keys + n->num_keys < capacity)
		return coalesce_nodes(n, neighbor, neighbor_index, k_prime);

	/* Redistribution. */

	else
		return redistribute_nodes(n, neighbor, neighbor_index, k_prime_index, k_prime);
}

template <class keytype> bnode<keytype> *bptree<keytype>::remove(keytype key){

	bnode<keytype> *key_leaf;
	blk_addr key_record;
	bnode<keytype> *temp;

	if((key_record = find(key)) < 0){
		freemem(false);
		return NULL;
	}
	if(!(key_leaf = find_leaf(key))){
		freemem(false);
		return NULL;
	}

	temp = delete_entry(key_leaf, key, key_record);
	if(temp){ 
		freemem(true);
		update_rblk(temp->baddr);
	}
	else freemem(false);
	return temp;
}

#endif