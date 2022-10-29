#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
  // printf("size of inode_t: %ld\n", sizeof(inode_t)); //424
}

void disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  static blockid_t last = IBLOCK(INODE_NUM, sb.nblocks);
  blockid_t begin = IBLOCK(INODE_NUM, sb.nblocks) + 1;
  for (blockid_t i = last + 1; i < BLOCK_NUM; i++)
  {
    if (!using_blocks[i])
    {
      using_blocks[i] = true;
      last = i;
      return i;
    }
  }
  for (blockid_t i = begin; i <= last; i++)
  {
    if (!using_blocks[i])
    {
      using_blocks[i] = true;
      last = i;
      return i;
    }
  }
  printf("\tbm(alloc_block): error! alloc_block failed! no enough space!\n");
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id] = 0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  static int inum = 0;
  for (int i = 0; i < INODE_NUM; i++)
  {
    inum = inum % INODE_NUM + 1;
    inode_t *ino = get_inode(inum);
    if (!ino)
    {
      ino = new inode_t;
      bzero(ino, sizeof(inode_t));
      ino->type = type;
      ino->size = 0;
      ino->atime = (unsigned int)time(NULL);
      ino->ctime = (unsigned int)time(NULL);
      ino->mtime = (unsigned int)time(NULL);
      put_inode(inum, ino);
      free(ino);
      break;
    }
    free(ino);
  }
  return inum;
}

void inode_manager::free_inode(uint32_t inum)
{
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *ino = get_inode(inum);
  if (ino == NULL)
    return;
  ino->type = 0;
  put_inode(inum, ino);
  free(ino);
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
inode_t *inode_manager::get_inode(uint32_t inum)
{
  inode_t *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);
  if (inum <= 0 || inum > INODE_NUM)
  {
    printf("\tim: (get_node) inum is out of range[1,INODE_NUM].\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (inode_t *)buf + inum % IPB;

  if (ino_disk->type == 0)
  {
    // printf("\tim: (get node) inode doesn't exist.\n");
    return NULL;
  }

  inode_t *ino = new inode_t;
  *ino = *ino_disk;
  return ino;
}

void inode_manager::put_inode(uint32_t inum, inode_t *ino)
{
  char buf[BLOCK_SIZE];
  inode_t *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (inode_t *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

blockid_t inode_manager::get_nth_block(inode_t *ino, uint32_t n)
{
  if (ino == NULL)
  {
    printf("\tim(get_nth_block): inode is NULL!.");
    return 0;
  }
  if (n < 0 || n >= MAXFILE)
  {
    printf("\tim(get_nth_block): nth(%d) is out of range!.", n);
    return 0;
  }
  if (n < NDIRECT)
  {
    return ino->blocks[n];
  }
  // NDIRECT <= n < MAXFILE
  uint32_t num = NDIRECT + (n - NDIRECT) / NINDIRECT;
  blockid_t id = ino->blocks[num];
  char buf[BLOCK_SIZE];
  bm->read_block(id, buf);
  return ((blockid_t *)buf)[n - NDIRECT];
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t *ino = get_inode(inum);
  if (ino == NULL)
  {
    printf("\tim(read_fild): didn't find inode %d\n", inum);
    return;
  }
  *size = ino->size;
  *buf_out = (char *)malloc(*size);
  uint32_t block_num = ino->size / BLOCK_SIZE;
  uint32_t remain_size = ino->size % BLOCK_SIZE;
  for (uint32_t i = 0; i < block_num; i++)
  {
    blockid_t id = get_nth_block(ino, i);
    bm->read_block(id, *buf_out + BLOCK_SIZE * i);
  }
  if (remain_size)
  {
    char buf[BLOCK_SIZE];
    blockid_t id = get_nth_block(ino, block_num);
    bm->read_block(id, buf);
    memcpy(*buf_out + BLOCK_SIZE * block_num, buf, remain_size);
  }
  ino->atime = time(NULL);
  put_inode(inum, ino);
  free(ino);
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf
   * is larger or smaller than the size of original inode
   */
  printf("\tim(wrirte_fild): write %d bytes to inode %d\n", size, inum);
  if (size < 0 || (uint64_t)size > MAXFILE * BLOCK_SIZE)
  {
    printf("\tim(wrirte_fild): size is out of range[0,%ld]\n", MAXFILE * BLOCK_SIZE);
    return;
  }
  inode_t *ino = get_inode(inum);
  if (ino == NULL)
  {
    printf("\tim(wrirte_fild): inode %d doesn't exist!\n", inum);
    return;
  }

  // reset the block number
  uint32_t old_blocks = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  uint32_t new_blocks = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  if (old_blocks < new_blocks)
  {
    for (uint32_t i = old_blocks; i < new_blocks; i++)
      alloc_nth_block(ino, i);
  }
  else if (old_blocks > new_blocks)
  {
    for (uint32_t i = new_blocks; i < old_blocks; i++)
      free_nth_block(ino, i);
  }

  ino->size = size;
  ino->atime = ino->mtime = ino->ctime = time(NULL);

  // write blocks;
  uint32_t block_num = size / BLOCK_SIZE;
  uint32_t remain_size = size % BLOCK_SIZE;
  for (uint32_t i = 0; i < block_num; i++)
  {
    blockid_t id = get_nth_block(ino, i);
    bm->write_block(id, buf + BLOCK_SIZE * i);
  }
  if (remain_size)
  {
    char tmp[BLOCK_SIZE] = {0};
    blockid_t id = get_nth_block(ino, block_num);
    memcpy(tmp, buf + block_num * BLOCK_SIZE, remain_size);
    bm->write_block(id, tmp);
  }

  put_inode(inum, ino);
  free(ino);
  return;
}

void inode_manager::alloc_nth_block(inode_t *ino, uint32_t n)
{
  if (ino == NULL)
  {
    printf("\tim(alloc_nth_block): inode is NULL!.");
    return;
  }
  if (n < 0 || n >= MAXFILE)
  {
    printf("\tim(alloc_nth_block): nth(%d) is out of range!.", n);
    return;
  }
  if (n < NDIRECT)
  {
    ino->blocks[n] = bm->alloc_block();
    return;
  }
  // NDIRECT <= n < MAXFILE
  uint32_t num = NDIRECT + (n - NDIRECT) / NINDIRECT;
  if (!ino->blocks[num])
  {
    printf("\tim(alloc_nth_block): alloc new INDIRECT BLOCK!\n");
    ino->blocks[num] = bm->alloc_block();
  }
  char buf[BLOCK_SIZE];
  bm->read_block(ino->blocks[num], buf);
  ((blockid_t *)buf)[n - NDIRECT] = bm->alloc_block();
  bm->write_block(ino->blocks[num], buf);
}

void inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *ino_disk;

  printf("\tim: get_attr %d\n", inum);

  ino_disk = get_inode(inum);
  if (!ino_disk)
    return;
  a.atime = ino_disk->atime;
  a.ctime = ino_disk->ctime;
  a.mtime = ino_disk->mtime;
  a.size = ino_disk->size;
  a.type = ino_disk->type;
  free(ino_disk);
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *ino = get_inode(inum);
  if (ino == NULL)
    return;

  uint32_t block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  // uint32_t remain_size = size % BLOCK_SIZE;
  for (uint32_t i = 0; i < block_num; i++)
    free_nth_block(ino, i);
  if (block_num > NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);

  free_inode(inum);
  free(ino);
  return;
}

void inode_manager::free_nth_block(inode_t *ino, uint32_t n)
{
  bm->free_block(get_nth_block(ino, n));
}
