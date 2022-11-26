/* fs.c: SimpleFS file system */

#include "sfs/fs.h"
#include "sfs/logging.h"
#include "sfs/utils.h"

#include <stdio.h>
#include <string.h>

/* Internal Functions */
void fs_initialize_free_block_bitmap(FileSystem *fs);
bool fs_load_inode(FileSystem *fs, size_t inode_number, Inode *node);
bool fs_save_inode(FileSystem *fs, size_t inode_number, Inode *node);

size_t find_free_block(FileSystem *fs);
/* External Functions */

/**
 * Debug FileSystem by doing the following:
 *
 *  1. Read SuperBlock and report its information.
 *
 *  2. Read Inode Table and report information about each Inode.
 *
 * @param       disk        Pointer to Disk structure.
 **/
void    fs_debug(Disk *disk) {
    Block block;

    /* Read SuperBlock */
    if (disk_read(disk, 0, block.data) == DISK_FAILURE) {
        return;
    }

    printf("SuperBlock:\n");
    printf("    magic number is %s\n",
        (block.super.magic_number == MAGIC_NUMBER) ? "valid" : "invalid");
    printf("    %u blocks\n"         , block.super.blocks);
    printf("    %u inode blocks\n"   , block.super.inode_blocks);
    printf("    %u inodes\n"         , block.super.inodes);

    /* Read Inodes */

    for (int inode_block = 1; inode_block <= block.super.inode_blocks; inode_block++) {
        Block inode_blk;
        disk_read(disk,inode_block,inode_blk.data);

        for (int inode = 0; inode < INODES_PER_BLOCK; inode++){
            if (!inode_blk.inodes[inode].valid) continue;
            printf("Inode %d:\n", (inode_block-1)*INODES_PER_BLOCK + inode);
            printf("    size: %u bytes\n", inode_blk.inodes[inode].size);
            printf("    direct blocks:");

            // all direct inodes
            for (int dp = 0; dp < POINTERS_PER_INODE; dp++) {
                if (inode_blk.inodes[inode].direct[dp] == 0) continue;
                printf(" %u", inode_blk.inodes[inode].direct[dp]);
            }
            printf("\n");

        // all the blocks from the indirect inode
            if (inode_blk.inodes[inode].indirect != 0) {
                printf("    indirect block: %u\n", inode_blk.inodes[inode].indirect);
                Block ind_blk;
                disk_read(disk, inode_blk.inodes[inode].indirect, ind_blk.data);
                printf("    indirect data blocks:");
                for (int ip = 0; ip < POINTERS_PER_BLOCK; ip++) {
                    if (ind_blk.pointers[ip] == 0) continue;
                    printf(" %u", ind_blk.pointers[ip]);
                }
                printf("\n");
            }
        }
    }
}

/**
 * Format Disk by doing the following:
 *
 *  1. Write SuperBlock (with appropriate magic number, number of blocks,
 *  number of inode blocks, and number of inodes).
 *
 *  2. Clear all remaining blocks.
 *
 * Note: Do not format a mounted Disk!
 *
 * @param       fs      Pointer to FileSystem structure.
 * @param       disk    Pointer to Disk structure.
 * @return      Whether or not all disk operations were successful.
 **/
bool    fs_format(FileSystem *fs, Disk *disk) {
    if (!fs) return false;
    if (!disk) return false;
    if (fs->disk != 0) return false;

    if(fs->free_blocks != NULL) {
        return false;
    }
    Block format_block;
    format_block.super.magic_number = MAGIC_NUMBER;
    format_block.super.blocks = disk->blocks;

    if (format_block.super.blocks % 10 == 0) {
        format_block.super.inode_blocks = disk->blocks / 10;
    } else {
        format_block.super.inode_blocks = disk->blocks / 10 + 1;
    }

    format_block.super.inodes = format_block.super.inode_blocks*INODES_PER_BLOCK;

    char buffer[BLOCK_SIZE] = {0};
    for (size_t i = 1; i < disk->blocks; i++) { 
        if (!disk_write(disk, i, buffer)) return false;
    }
    //fs->disk = 0;

    return true;
}

/**
 * Mount specified FileSystem to given Disk by doing the following:
 *
 *  1. Read and check SuperBlock (verify attributes).
 *
 *  2. Verify and record FileSystem disk attribute. 
 *
 *  3. Copy SuperBlock to FileSystem meta data attribute
 *
 *  4. Initialize FileSystem free blocks bitmap.
 *
 * Note: Do not mount a Disk that has already been mounted!
 *
 * @param       fs      Pointer to FileSystem structure.
 * @param       disk    Pointer to Disk structure.
 * @return      Whether or not the mount operation was successful.
 **/
bool    fs_mount(FileSystem *fs, Disk *disk) {
    //if (fs->disk != disk) return false;
    if (fs->disk == disk) return false;
    //if (fs->disk != 0) return false;

    Block disk_super_block;
    if (!disk_read(disk,0,disk_super_block.data)) return false;

    SuperBlock *sb = &disk_super_block.super;

    if (!sb) return false;


    // verify attributes of superblock
    if (sb->magic_number != MAGIC_NUMBER) return false;
    if (sb->inode_blocks*INODES_PER_BLOCK > sb->inodes) return false;
    if (sb->blocks < 3) return false;
    if (sb->inode_blocks < sb->blocks / 10) return false;


    fs->meta_data.magic_number = sb->magic_number;
    fs->meta_data.inode_blocks = sb->inode_blocks;
    fs->meta_data.blocks = sb->blocks;
    fs->meta_data.inodes = sb->inodes;

    fs->disk = disk;

    // Initializing FileSystem free blocks bitmap
    fs_initialize_free_block_bitmap(fs); 
    return true;
}

/**
 * Unmount FileSystem from internal Disk by doing the following:
 *
 *  1. Set FileSystem disk attribute.
 *
 *  2. Release free blocks bitmap.
 *
 * @param       fs      Pointer to FileSystem structure.
 **/
void    fs_unmount(FileSystem *fs) {
    if (!fs) return;
    fs->disk = 0;
    if (fs->free_blocks) free(fs->free_blocks);
    fs->free_blocks = NULL; 
}

/**
 * Allocate an Inode in the FileSystem Inode table by doing the following:
 *
 *  1. Search Inode table for free inode.
 *
 *  2. Reserve free inode in Inode table.
 *
 * Note: Be sure to record updates to Inode table to Disk.
 *
 * @param       fs      Pointer to FileSystem structure.
 * @return      Inode number of allocated Inode.
 **/
ssize_t fs_create(FileSystem *fs) {
    ssize_t inode_count = 0;
    for (int inode_block = 1; inode_block <= fs->meta_data.inode_blocks; inode_block++) {
        Block block;
        disk_read(fs->disk,inode_block,block.data);
          // going through the inodes in a block
        for (int inode = 0; inode < INODES_PER_BLOCK; inode++) {
            if (!block.inodes[inode].valid) {// && !fs->free_blocks[inode_count]) { 
                block.inodes[inode].valid = true;
                //fs->free_blocks[inode_block] = false;
                //for some reason ^ creates memory errors
                disk_write(fs->disk,inode_block, block.data);
             
                return inode_count;
            }
            inode_count += 1;
        }
    }
    return -1;
}

/**
 * Remove Inode and associated data from FileSystem by doing the following:
 *
 *  1. Load and check status of Inode.
 *
 *  2. Release any direct blocks.
 *
 *  3. Release any indirect blocks.
 *
 *  4. Mark Inode as free in Inode table.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to remove.
 * @return      Whether or not removing the specified Inode was successful.
 **/
bool    fs_remove(FileSystem *fs, size_t inode_number) {

    // we need load inode and save inode here
    size_t block_num = inode_number / INODES_PER_BLOCK + 1;
    size_t inode_i = inode_number % INODES_PER_BLOCK;
    Block blk;
    disk_read(fs->disk, block_num, blk.data);
    //Inode *node;
    //fs_load_inode(fs,inode_number,node);
    //size_t inode_i = node.

    if (!blk.inodes[inode_i].valid) return false;

    // all direct inodes
    for (int dp = 0; dp < POINTERS_PER_INODE; dp++) {
        if (blk.inodes[inode_i].direct[dp] == 0) continue;
        // RELEASE BLOCKS and mark as free in inode table
        fs->free_blocks[blk.inodes[inode_i].direct[dp]] = true;
        blk.inodes[inode_i].direct[dp] = 0;
    }

    // all the blocks from the indirect inode
    if (blk.inodes[inode_i].indirect != 0) {
        Block ind_blk;
        disk_read(fs->disk, blk.inodes[inode_i].indirect, ind_blk.data);
        for (int ip = 0; ip < POINTERS_PER_BLOCK; ip++) {
            if (ind_blk.pointers[ip] == 0) continue;
            // RELEASE BLOCKS to dooooo and also free in inode table
            fs->free_blocks[ind_blk.pointers[ip]] = true;
            ind_blk.pointers[ip] = 0;
            
        }
        // marking block pointed to by indrect pointer as free
        fs->free_blocks[blk.inodes[inode_i].indirect] = true;
        disk_write(fs->disk, blk.inodes[inode_i].indirect, ind_blk.data);
        blk.inodes[inode_i].indirect = 0;
        
    }
    blk.inodes[inode_i].size = 0;
    blk.inodes[inode_i].valid = false;
    disk_write(fs->disk, block_num, blk.data);


 
    return true;
}

/**
 * Return size of specified Inode.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to remove.
 * @return      Size of specified Inode (-1 if does not exist).
 **/
ssize_t fs_stat(FileSystem *fs, size_t inode_number) {
    size_t block_num = inode_number / INODES_PER_BLOCK + 1;
    size_t inode_i = inode_number % INODES_PER_BLOCK;
    Block blk;
    disk_read(fs->disk, block_num, blk.data);
    if (blk.inodes[inode_i].valid) return blk.inodes[inode_i].size;

    return -1;
}

/**
    if (fs->disk == disk) return false;
    if (fs->disk == 0) return false;
 * Read from the specified Inode into the data buffer exactly length bytes
 * beginning from the specified offset by doing the following:
 *
 *  1. Load Inode information.
 *
 *  2. Continuously read blocks and copy data to buffer.
 *
 *  Note: Data is read from direct blocks first, and then from indirect blocks.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to read data from.
 * @param       data            Buffer to copy data to.
 * @param       length          Number of bytes to read.
 * @param       offset          Byte offset from which to begin reading.
 * @return      Number of bytes read (-1 on error).
 **/
ssize_t fs_read(FileSystem *fs, size_t inode_number, char *data, size_t length, size_t offset) {
    size_t block_num = inode_number / INODES_PER_BLOCK + 1;
    size_t inode_i = inode_number % INODES_PER_BLOCK;
    size_t ncopy;
    Block blk;
    disk_read(fs->disk, block_num, blk.data);

    // adjust length to account for offset
    // change length if size of file is < length + offset
    if (blk.inodes[inode_i].size < (length + offset)) {
        length = blk.inodes[inode_i].size - offset;
    }

    size_t data_block = offset / BLOCK_SIZE;
    ssize_t nread = 0;
    size_t data_offset = offset % BLOCK_SIZE;

    //size_t ncopy;
    // if length < block size then ncopy is length - offset
    if (length < BLOCK_SIZE) {
        ncopy = length;
        //ncopy = length - offset;
    } else {
        ncopy = BLOCK_SIZE - data_offset;
        //ncopy = blk.inodes[inode_i].size - offset;
    }
    

    
    while (nread < length) {
        Block data_blk = {{0}};
        Block indirect_blk = {{0}};
        if (data_block < POINTERS_PER_INODE) {
            if (disk_read(fs->disk, blk.inodes[inode_i].direct[data_block], data_blk.data) == DISK_FAILURE) return -1;
        } else {
            size_t indirect_offset = data_block - POINTERS_PER_INODE;
            if (disk_read(fs->disk, blk.inodes[inode_i].indirect, indirect_blk.data) == DISK_FAILURE) return -1; 
            if (disk_read(fs->disk, indirect_blk.pointers[indirect_offset], data_blk.data) == DISK_FAILURE) return -1;

        }

        memcpy(data + nread, data_blk.data + data_offset, ncopy);

        data_offset = 0;
        data_block++;

        nread += ncopy;

        if ((length - nread) > BLOCK_SIZE) {
            ncopy = BLOCK_SIZE;
        } else {
            ncopy = length - nread;
        }
    }

    return nread;
}

/**
 * Write to the specified Inode from the data buffer exactly length bytes
 * beginning from the specified offset by doing the following:
 *
 *  1. Load Inode information.
 *
 *  2. Continuously copy data from buffer to blocks.
 *
 *  Note: Data is read from direct blocks first, and then from indirect blocks.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to write data to.
 * @param       data            Buffer with data to copy
 * @param       length          Number of bytes to write.
 * @param       offset          Byte offset from which to begin writing.
 * @return      Number of bytes read (-1 on error).
 **/
ssize_t fs_write(FileSystem *fs, size_t inode_number, char *data, size_t length, size_t offset) {

    size_t block_num = inode_number / INODES_PER_BLOCK + 1;
    size_t inode_i = inode_number % INODES_PER_BLOCK;
    Block blk;
    disk_read(fs->disk, block_num, blk.data);

    //if (blk.inodes[inode_i].size < (length + offset)) {
    //    length = blk.inodes[inode_i].size - offset;
    //}

    size_t data_block = offset / BLOCK_SIZE;
    ssize_t nwrite = 0;
    size_t data_offset = offset % BLOCK_SIZE;

    size_t ncopy;
    // adjust length to account for offset
    if (length < BLOCK_SIZE) {
        ncopy = length;
    } else {
        ncopy = BLOCK_SIZE - data_offset;
    } 

    // check if valid inode
    if (!blk.inodes[inode_i].valid) return -1;
    
    while (nwrite < length) {
        Block data_blk = {{0}};
        Block indirect_blk = {{0}};
        
        size_t new_block = 0;

        if (data_block < POINTERS_PER_INODE) {

            // allocate new block if not allocated (direct pointer)
            if (blk.inodes[inode_i].direct[data_block] == 0) {
                new_block = find_free_block(fs);
                if (new_block == 0) return -1; // if there was no free block found
                blk.inodes[inode_i].direct[data_block] = new_block;
            }

            // read dirptr to data block
            if (disk_read(fs->disk, blk.inodes[inode_i].direct[data_block], data_blk.data) == DISK_FAILURE) return -1;
            // copy new data into data block
            memcpy(data_blk.data + data_offset, data + nwrite, ncopy);
            // write the updated data block to disk
            if (disk_write(fs->disk, blk.inodes[inode_i].direct[data_block], data_blk.data) == DISK_FAILURE) return -1;
            
        } else {
            size_t indirect_offset = data_block - POINTERS_PER_INODE;
            
            // allocate new block if not allocated (indirect pointer)
            if (blk.inodes[inode_i].indirect == 0) {
                new_block = find_free_block(fs);
                if (new_block == 0) return -1;
                blk.inodes[inode_i].indirect = new_block;
            } else {
                // read indirect pointer
                if (disk_read(fs->disk, blk.inodes[inode_i].indirect, indirect_blk.data) == DISK_FAILURE) return -1; 
            }

            // allocate new block if not allocated (direct pointers in indirect pointer)
            if (indirect_blk.pointers[indirect_offset] == 0) {
                new_block = find_free_block(fs);
                if (new_block == 0) return -1;
                indirect_blk.pointers[indirect_offset] = new_block;
                if (disk_write(fs->disk, blk.inodes[inode_i].indirect, indirect_blk.data) == DISK_FAILURE) return -1;
            } else {
                // read direct pointers from indirect pointer into data block
                if (disk_read(fs->disk, indirect_blk.pointers[indirect_offset], data_blk.data) == DISK_FAILURE) return -1;
            }

            // copy data into data block
            memcpy(data_blk.data + data_offset, data + nwrite, ncopy);
            // write the updated data block to disk
            if (disk_write(fs->disk, indirect_blk.pointers[indirect_offset], data_blk.data) == DISK_FAILURE) return -1;

        }

        data_offset = 0;
        data_block++;

        nwrite += ncopy;

        if ((length - nwrite) > BLOCK_SIZE) {
            ncopy = BLOCK_SIZE;
        } else {
            ncopy = length - nwrite;
        }

        // updating inode in here?
        if (offset + nwrite > blk.inodes[inode_i].size) {
            blk.inodes[inode_i].size = offset + nwrite;
        }

        disk_write(fs->disk, block_num, blk.data);
    }

    // update inode size !!!!
    //if (offset + nwrite > blk.inodes[inode_i].size) {
    //    blk.inodes[inode_i].size = offset + nwrite;
    //}
    //disk_write(fs->disk, block_num, blk.data);
    return nwrite;

}

size_t find_free_block(FileSystem *fs) {
    bool *free_blocks = fs->free_blocks;
    for (int i = 0; i < fs->meta_data.blocks; i++) {
        if (free_blocks[i]) {
            free_blocks[i] = false;
            return i;
        }
    }
    return 0;
}



void fs_initialize_free_block_bitmap(FileSystem *fs) { 
    
    bool *free_blocks = calloc(fs->meta_data.blocks, sizeof(bool));
    for (int i = 0; i < fs->meta_data.blocks; i++) {
        free_blocks[i] = true;
    }
    fs->free_blocks = free_blocks;
    fs->free_blocks[0] = false;

    for (int inode_block = 1; inode_block <= fs->meta_data.inode_blocks; inode_block++) {
        Block block;
        if (disk_read(fs->disk,inode_block,block.data) == DISK_FAILURE) return;
          // going through the inodes in a block
        for (int inode = 0; inode < INODES_PER_BLOCK; inode++) {
            if (block.inodes[inode].valid) { //what does valid even mean

                // going through direct pointers in the inode
                for (int dp = 0; dp < POINTERS_PER_INODE; dp++) {
                    // if an inode is not 0, mark that data block as being not free
                    if (block.inodes[inode].direct[dp] != 0) {
                         free_blocks[block.inodes[inode].direct[dp]] = false;
                    }
                }
                // if there is a valid indirect block
                if (block.inodes[inode].indirect != 0) {
                    free_blocks[block.inodes[inode].indirect] = false; // mark the indirect data block as not free
                    Block indirect_block;

                    // reading from the indirect pointer
                    if (disk_read(fs->disk, block.inodes[inode].indirect, indirect_block.data) == DISK_FAILURE) return;

                    // go through all the pointers in the block (separate block with all direct pointers)
                    for (int p = 0; p < POINTERS_PER_BLOCK; p++) {
                        //accessing all the indirect block pointers to things
                        if (indirect_block.pointers[p] != 0) {
                            free_blocks[indirect_block.pointers[p]] = false; // mark all the data blocks pointed to from the indirect one as not free
                        }
                    }
                }
            }
        }
        free_blocks[inode_block] = false;
    }
}
/*
bool fs_load_inode(FileSystem *fs, size_t inode_number, Inode *node) {   
    size_t block_num = inode_number / INODES_PER_BLOCK + 1;
    size_t inode_i = inode_number % INODES_PER_BLOCK;
    Block blk;

    disk_read(fs->disk, block_num, blk.data);

}*/

/*bool fs_save_inode(FileSystem *fs, size_t inode_number, Inode *node) {
}*/


/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
