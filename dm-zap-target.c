// SPDX-License-Identifier: GPL-2.0
/*
 * Copyright (C) 2019 Western Digital Corporation or its affiliates.
 *
 */

#include "dm-zap.h"

#include <linux/module.h>
#include <linux/kobject.h>
#include <linux/sysfs.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <linux/string.h> 


/* TODO: this might not work for us. Calculate the need */
#define DMZAP_MIN_BIOS		8192

static struct kobject *zap_stat_kobject;
struct dmzap_target* dmzap_ptr;
struct mutex heap_increase_lock;

static ssize_t reset_zap_stats(struct kobject *kobj, struct kobj_attribute *attr,
                      const char *buf, size_t count)
{
	dmzap_ptr->nr_user_written_sec = 0;
	dmzap_ptr->nr_gc_written_sec = 0;
        return count;
}

static struct kobj_attribute zap_reset_attribute =__ATTR(reset_stats, 0220, NULL,
                                                   reset_zap_stats);

/* Initializes the bio context. */
static inline void dmzap_init_bioctx(struct dmzap_target *dmzap,
				     struct bio *bio)
{
    /* Extracts the pointer to private per bio data. */
	struct dmzap_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmzap_bioctx));
    /* Sets target. */
	bioctx->target = dmzap;
    /* Sets user sector to be processed. */
	bioctx->user_sec = bio->bi_iter.bi_sector;
    /* Sets bio. */
	bioctx->bio = bio;
    /* Atomically sets the reference count of bioctx to 1. */
	refcount_set(&bioctx->ref, 1);
}

/*
 * Target bio completion.
 */
inline void dmzap_bio_endio(struct bio *bio, blk_status_t status)
{
    /* Extracts the pointer to private per bio data. */
	struct dmzap_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmzap_bioctx));

	if (status != BLK_STS_OK && bio->bi_status == BLK_STS_OK)
		bio->bi_status = status;
	if (bio->bi_status != BLK_STS_OK)
		bioctx->target->dev->flags |= DMZ_CHECK_BDEV;

	if (refcount_dec_and_test(&bioctx->ref)) {
		// struct dm_zone *zone = bioctx->zone; //TODO do we need that?
		//
		// if (zone) {
		// 	if (bio->bi_status != BLK_STS_OK &&
		// 	    bio_op(bio) == REQ_OP_WRITE)
		// 		set_bit(DMZ_SEQ_WRITE_ERR, &zone->flags);
		// 	dmz_deactivate_zone(zone);
		// }
		bio_endio(bio);
	}
}

/* Gets the sequential write pointer (sector to be written). */
sector_t dmzap_get_seq_wp(struct dmzap_target *dmzap)
{
	return dmzap->dmzap_zones[dmzap->dmzap_zone_wp].zone->wp;
}

/* report_zones handle for dmzap_type - Reports zone information for dmzap_target. */
static int dmzap_report_zones(struct dm_target *ti,
		struct dm_report_zones_args *args, unsigned int nr_zones)
{
    struct dmzap_target *dmzap = ti->private;
    struct dmz_dev *dev = dmzap->dev;
    int ret;

    /* Pipe the request to the backing block device. */
    ret = dm_report_zones(dev->bdev, 0, 0, args, nr_zones);
    if (ret != 0)
        return ret;

	return 0;
}

/* Callback function for BLKREPORTZONE ioctl. */
static int dmzap_init_zone(struct blk_zone *blkz, unsigned int idx, void *data)
{
	struct dmzap_target *dmzap = data;
	struct dmz_dev *dev = dmzap->dev;

    /* If the reported length is not equal to the derived length in initialization, fail. */
	if (blkz->len != dev->zone_nr_sectors) {
		if (blkz->start + blkz->len == dev->capacity)
			return 0;
		return -ENXIO;
	}
	return 0;
}

/* Initializes dmzap zone descriptors and BLKREPORTZONE ioctl zone descriptors. */
int dmzap_zones_init(struct dmzap_target *dmzap)
{
	struct dmz_dev *dev = dmzap->dev;
	unsigned int i, nr_zones;
	sector_t sector = 0;
	int ret;

	atomic_set(&dmzap->header_seq_nr, 0);

    /* Allocates memory for BLKREPORTZONE ioctl zone descriptors. */
	dmzap->internal_zones = kvmalloc_array(dmzap->nr_internal_zones,
			sizeof(struct blk_zone), GFP_KERNEL | __GFP_ZERO);
	if (!dmzap->internal_zones)
		return -ENOMEM;

    /* Allocates memory for dmzap zone descriptors. */
	dmzap->dmzap_zones = kvmalloc_array(dmzap->nr_internal_zones,
			sizeof(struct dmzap_zone), GFP_KERNEL | __GFP_ZERO);
	if (!dmzap->dmzap_zones)
		goto err_free_internal_zones;

    /* Allocates memory for pointers of user-exposed zones. */
	dmzap->user_zones = kvmalloc_array(dmzap->nr_user_exposed_zones,
			sizeof(struct dmzap_zone *), GFP_KERNEL | __GFP_ZERO);
	if (!dmzap->user_zones)
		goto err_free_dmzap_zones;

    /* Allocates memory for pointers of overprovisioning zones. */
	dmzap->op_zones = kvmalloc_array(dmzap->nr_op_zones,
			sizeof(struct dmzap_zone *), GFP_KERNEL | __GFP_ZERO);
	if (!dmzap->op_zones)
		goto err_free_user_zones;

    /* Allocates memory for pointers of metadata zones. */
	dmzap->meta_zones = kvmalloc_array(dmzap->nr_meta_zones,
				sizeof(struct dmzap_zone *), GFP_KERNEL | __GFP_ZERO);
	if (!dmzap->meta_zones)
		goto err_free_op_zones;

	sector = 0;
	for (i = 0; i < dmzap->nr_internal_zones ; i++) {
        /* Is going to manipulate ith BLKREPORTZONE ioctl zone descriptor. */
		struct blk_zone *zone = &dmzap->internal_zones[i];
        /**
         * Sets the start and write pointer of the zone descriptor to the begining sector
         * of the zone. */
		zone->start = zone->wp = sector;
        /* Increases the sector counter by the number of sectors in the zone. */
		sector += dev->zone_nr_sectors;
        /* Sets the number of sectors in the zone. */
		zone->len = dev->zone_nr_sectors;
        /* Sets the type of the zone (sequential-only). */
		zone->type = BLK_ZONE_TYPE_SEQWRITE_REQ;
        /* Sets the condition of the zone (empty). */
		zone->cond = BLK_ZONE_COND_EMPTY;
        /**
         * Sets the capacity of the zone (in zoned devices with unalinged zones, this number
         * can be smaller than the actual zone size). */
		zone->capacity = dev->zone_nr_sectors; //TODO ZNS capacity: the capacity of the backing zone has to be set individually ?

        /* Sets the backing-zone of dmzap zone descriptor to the BLKREPORTZONE ioctl zone descriptor. */
		dmzap->dmzap_zones[i].zone = zone;
        /* Sets the type of dmzap zone descriptor. */
		if(i < (dmzap->nr_user_exposed_zones)){
			dmzap->dmzap_zones[i].type = DMZAP_RAND;
		}else{
			dmzap->dmzap_zones[i].type = DMZAP_RAND; //DMZAP_OP;
		}
        /* Sets the sequential number of dmzap zone descriptor. */
		dmzap->dmzap_zones[i].seq = i;
        /* Sets the state of dmzap zone descriptor (clean). */
		dmzap->dmzap_zones[i].state = DMZAP_CLEAN;
        /* Sets the number of invalid blocks of dmzap zone descriptor. */
		dmzap->dmzap_zones[i].nr_invalid_blocks = 0;
        /* Sets the last modification time of dmzap zone descriptor. */
		dmzap->dmzap_zones[i].zone_age = 0;
        /* Sets the shift time of dmzap zone descriptor. */
		dmzap->dmzap_zones[i].shift_time = 0;
        /* Sets the cost-benefit of dmzap zone descriptor. */
		dmzap->dmzap_zones[i].cb = -1;
        /* Sets the reclaim class of dmzap zone descriptor. */
		dmzap->dmzap_zones[i].reclaim_class = -1;
        /* Initializes an empty linked list. */
		INIT_LIST_HEAD(&dmzap->dmzap_zones[i].link);
        /* Initializes an empty red-black tree. */
		RB_CLEAR_NODE(&dmzap->dmzap_zones[i].node);
        /* Initializes the reclaim class mutex lock. */
		mutex_init(&dmzap->dmzap_zones[i].reclaim_class_lock);
	}

    /* Sets the state of the first zone to explicitly open. */
	if (dmzap->nr_internal_zones) {
		dmzap->dmzap_zones[0].state = DMZAP_OPENED;
		dmzap->dmzap_zones[0].zone->cond = BLK_ZONE_COND_EXP_OPEN;
	}

	/* Splits dmzap zones between user zones and overprovisioning zones. */
	for (i = 0; i < dmzap->nr_internal_zones ; i++) {
		if(i < (dmzap->nr_user_exposed_zones)){
				dmzap->user_zones[i] = &dmzap->dmzap_zones[i];
		} else {
				dmzap->op_zones[i - dmzap->nr_user_exposed_zones] = &dmzap->dmzap_zones[i];
		}
	}

	nr_zones = dmzap->nr_internal_zones;

	/**
     * BLKREPORTZONE ioctl for the backing zoned block device. Its report is used to check
     * whether device information was correctly recieved during initialization. 
     */
	ret = blkdev_report_zones(dev->bdev, 0, nr_zones,
		dmzap_init_zone, dmzap);

	if (ret < 0) {
		dmz_dev_err(dev, "failed to get internal zone descriptors");
		goto err_free_op_zones;
	}

    /* Initializes zone write pointer of dmzap. */
	dmzap->dmzap_zone_wp = 0;
    /* Initializes debug int of dmzap. */
	dmzap->debug_int = 0;

	return 0;

err_free_op_zones:
	kvfree(dmzap->op_zones);
err_free_user_zones:
	kvfree(dmzap->user_zones);
err_free_dmzap_zones:
	kvfree(dmzap->dmzap_zones);
err_free_internal_zones:
	kvfree(dmzap->internal_zones);

	return -ENOMEM;
}

/* Frees backing zone and dmzap zone descriptors. */
void dmzap_zones_free(struct dmzap_target *dmzap)
{
	kvfree(dmzap->meta_zones);
	kvfree(dmzap->op_zones);
	kvfree(dmzap->user_zones);
	kvfree(dmzap->dmzap_zones);
	kvfree(dmzap->internal_zones);
}

/* Populates geometry fields of dmzap_target. */
int dmzap_geometry_init(struct dm_target *ti)
{
	struct dmzap_target *dmzap = ti->private;
	struct dmz_dev *dev = dmzap->dev;
	unsigned int dev_zones = dev->nr_zones;
	unsigned int op;

    /**
     * Calculates the number of overprovisioning zones base on the initialized overprovisioning
     * rate.
     */
	if (dev_zones > 1 )
		op = dev_zones * dmzap->overprovisioning_rate / 100;
	else
		op = 0;

    /* Sets the number of internal zones of dmzap_target. */
	dmzap->nr_internal_zones = dev_zones;
    /* Sets the number of overprovisioning zones of dmzap_target. */
	dmzap->nr_op_zones = op;
    /* Sets the number of metadata zones of dmzap_target. */
	dmzap->nr_meta_zones = 0; //TODO check how much meta zones are needed.
    /* Sets the number of user-exposed zones of dmzap_target. */
	dmzap->nr_user_exposed_zones = dmzap->nr_internal_zones
		- dmzap->nr_op_zones - dmzap->nr_meta_zones;

    /* Sets the user-exposed capacity (number of sectors) of dmzap_target. */
	dmzap->capacity = dmzap->nr_user_exposed_zones << ilog2(dev->zone_nr_sectors);
    /* Sets the device capacity (number of sectors) of dmzap_target. */
	dmzap->dev_capacity = dmzap->nr_internal_zones << ilog2(dev->zone_nr_sectors);

	return 0;
}

// /*
//  * Initialize zone header
//  */
// void dmzap_init_header(struct dmzap_target *dmzap, struct dmzap_zone_header *header)
// {
// 	header->magic = cpu_to_le32(DM_ZAP_MAGIC);
// 	header->version = cpu_to_le32(DM_ZAP_VERSION);
// 	header->seq = cpu_to_le32(atomic_read(&dmzap->header_seq_nr));
// 	atomic_inc(&dmzap->header_seq_nr);
// 	header->crc = 0; //TODO: calc real crc
// 	//header->crc = cpu_to_le32(crc32_le(0, (unsigned char *)header, DMZ_BLOCK_SIZE));
// }

/* Handles a bio inside a chunk. */
int dmzap_handle_bio(struct dmzap_target *dmzap,
				struct dmzap_chunk_work *cw, struct bio *bio)
{
	int ret;

    /* If block device is dying, return error. */
	if (dmzap->dev->flags & DMZ_BDEV_DYING) {
		ret = -EIO;
		goto out;
	}

    /* If bio has no sectors left, it is done. */
	if (!bio_sectors(bio)) {
		ret = DM_MAPIO_SUBMITTED;
		goto out;
	}

	/*
	 * Write may trigger a zone allocation. So make sure the
	 * allocation can succeed.
	 */
	if (bio_op(bio) == REQ_OP_WRITE){
		mutex_lock(&dmzap->reclaim_lock);
		dmzap_schedule_reclaim(dmzap);
		mutex_unlock(&dmzap->reclaim_lock);
		dmzap->nr_user_written_sec += bio_sectors(bio);
	}

	switch (bio_op(bio)) {
    /* In case of read, do the conventional read. */
	case REQ_OP_READ:
		mutex_lock(&dmzap->map.map_lock);
		ret = dmzap_conv_read(dmzap, bio);
		mutex_unlock(&dmzap->map.map_lock);
		break;
    /* In case of write, do the conventional write. */
	case REQ_OP_WRITE:
		mutex_lock(&dmzap->map.map_lock);
		ret = dmzap_conv_write(dmzap, bio);
		mutex_unlock(&dmzap->map.map_lock);

		break;
    /* In case of discard, do the discard. */
	case REQ_OP_DISCARD:
	case REQ_OP_WRITE_ZEROES:
		dmz_dev_debug(dmzap->dev, "Discard operation triggered");
		ret = dmzap_handle_discard(dmzap, bio);
		break;
	default:
		dmz_dev_err(dmzap->dev, "Ignoring unsupported BIO operation 0x%x",
			    bio_op(bio));
		ret = -EIO;
	}

	/* Updates the last access time of the dmzap. */
	dmzap_reclaim_bio_acc(dmzap);

    /* Periodically prints dmzap statistics. */
	if(time_is_before_jiffies(dmzap->wa_print_time + DMZAP_WA_PERIOD)){
		trace_printk("User written sectors: %lld, GC written sectors: %lld, vsm: %u, dev_c: %lld\n",
			dmzap->nr_user_written_sec,
			dmzap->nr_gc_written_sec,
			dmzap->victim_selection_method,
			dmzap->dev_capacity);
		dmzap->wa_print_time = jiffies;
	}
out:
	dmzap_bio_endio(bio, errno_to_blk_status(ret));
	return ret;
}

/*
 * Increment a chunk reference counter.
 */
static inline void dmzap_get_chunk_work(struct dmzap_chunk_work *cw)
{
	refcount_inc(&cw->refcount);
}

/* Decreases the reference count of a chunk work. Destroys it if becomes zero. */
static void dmzap_put_chunk_work(struct dmzap_chunk_work *cw)
{
    /* Decreases the refcount, and checks whether it is zero. */
	if (refcount_dec_and_test(&cw->refcount)) {
        /* Warns if the bio list is not empty yet. */
		WARN_ON(!bio_list_empty(&cw->bio_list));
        /* Deletes the chunk work from the chunk work radix tree. */
		radix_tree_delete(&cw->target->chunk_rxtree, cw->chunk);
		kfree(cw);
	}
}

/* Chunk work function, called by chunk work queue. */
static void dmzap_chunk_work_(struct work_struct *work)
{
	struct dmzap_chunk_work *cw = container_of(work, struct dmzap_chunk_work, work);
	struct dmzap_target *dmzap = cw->target;
	struct bio *bio;

	mutex_lock(&dmzap->chunk_lock);

	/* Process all bios in the bio list of the chunk work. */
	while ((bio = bio_list_pop(&cw->bio_list))) {
		mutex_unlock(&dmzap->chunk_lock);
        /* Handles the bio. */
		dmzap_handle_bio(dmzap, cw, bio);
		mutex_lock(&dmzap->chunk_lock);
        /* Decreases the chunk work refcount. */
		dmzap_put_chunk_work(cw);
	}

	/* Decreases the chunk work refcount, and destroys it. */
	dmzap_put_chunk_work(cw);

	mutex_unlock(&dmzap->chunk_lock);
}

/* Flush work function. Flushes all finished bios passed to its bio list. */
static void dmzap_flush_work(struct work_struct *work)
{
	struct dmzap_target *dmzap = container_of(work, struct dmzap_target, flush_work.work);
	struct bio *bio;
	int ret = 0;

	/* Processes queued flush bios. */
	while (1) {
		spin_lock(&dmzap->flush_lock);
        /* Gets a bio from its list. */
		bio = bio_list_pop(&dmzap->flush_list);
		spin_unlock(&dmzap->flush_lock);

        /* Breaks if there is no more bio. */
		if (!bio)
			break;

        /* Finishes the bio. */
		dmzap_bio_endio(bio, errno_to_blk_status(ret));
	}

    /* Reschedules itself to be run after DMZAP_FLUSH_PERIOD jiffies. */
	queue_delayed_work(dmzap->flush_wq, &dmzap->flush_work, DMZAP_FLUSH_PERIOD);
}

/**
 * Gets the existing chunk work for the chunk this bio is inside of, and adds the bio to
 * its bio list to be processed. If the chunk work did not exist, create it.
 */
static int dmzap_queue_chunk_work(struct dmzap_target *dmzap, struct bio *bio)
{
	unsigned int chunk = dmzap_bio_chunk(dmzap->dev, bio);
	struct dmzap_chunk_work *cw;
	int ret = 0;

	mutex_lock(&dmzap->chunk_lock);

	/* Looks up the chunk work for this bio in the chunk work radix tree. */
	cw = radix_tree_lookup(&dmzap->chunk_rxtree, chunk);
    /* If the chunk work did not exist, creates it. */
	if (!cw) {
		/* Allocates memory for the new chunk work. */
		cw = kmalloc(sizeof(struct dmzap_chunk_work), GFP_NOIO);
		if (unlikely(!cw)) {
			ret = -ENOMEM;
			goto out;
		}

        /* Initializes the chunk work. Assosiates it with its function. */
		INIT_WORK(&cw->work, dmzap_chunk_work_);
        /* Sets refcount of the chunk work to 1. */
		refcount_set(&cw->refcount, 1);
        /* Sets the target the chunk work. */
		cw->target = dmzap;
        /* Sets the chunk number the chunk work. */
		cw->chunk = chunk;
        /* Initializes the bio list of the chunk work. */
		bio_list_init(&cw->bio_list);

        /* Inserts the newly created chunck work to the chunk work radix tree. */
		ret = radix_tree_insert(&dmzap->chunk_rxtree, chunk, cw);
		if (unlikely(ret)) {
			kfree(cw);
			goto out;
		}
	}

    /* Adds current bio to the bio list of the chunk work. */
	bio_list_add(&cw->bio_list, bio);
    /* Increments the refcount of the chunk work. */
	dmzap_get_chunk_work(cw);

    /* Updates the last access time of the dmzap. */
	dmzap_reclaim_bio_acc(dmzap);
    /* If the work has not been queued before, queue it. */
	if (queue_work(dmzap->chunk_wq, &cw->work))
        /* Increments the refcount of the chunk work. */
		dmzap_get_chunk_work(cw);
out:
	mutex_unlock(&dmzap->chunk_lock);
	return ret;
}

/**
 * Check if the backing device is being removed. If it's on the way out,
 * start failing I/O. Reclaim and metadata components also call this
 * function to cleanly abort operation in the event of such failure.
 */
bool dmzap_bdev_is_dying(struct dmz_dev *dmz_dev)
{
	if (dmz_dev->flags & DMZ_BDEV_DYING)
		return true;

	if (dmz_dev->flags & DMZ_CHECK_BDEV)
		return !dmzap_check_bdev(dmz_dev);

	if (blk_queue_dying(bdev_get_queue(dmz_dev->bdev))) {
		dmz_dev_warn(dmz_dev, "Backing device queue dying");
		dmz_dev->flags |= DMZ_BDEV_DYING;
	}

	return dmz_dev->flags & DMZ_BDEV_DYING;
}

/**
 * Check the backing device availability. This detects such events as
 * backing device going offline due to errors, media removals, etc.
 * This check is less efficient than dmzap_bdev_is_dying() and should
 * only be performed as a part of error handling.
 */
bool dmzap_check_bdev(struct dmz_dev *dmz_dev)
{
	struct gendisk *disk;

	dmz_dev->flags &= ~DMZ_CHECK_BDEV;

	if (dmzap_bdev_is_dying(dmz_dev))
		return false;

	disk = dmz_dev->bdev->bd_disk;
	if (disk->fops->check_events &&
	    disk->fops->check_events(disk, 0) & DISK_EVENT_MEDIA_CHANGE) {
		dmz_dev_warn(dmz_dev, "Backing device offline");
		dmz_dev->flags |= DMZ_BDEV_DYING;
	}

	return !(dmz_dev->flags & DMZ_BDEV_DYING);
}

/* map handle for dmzap_type - Processes a bio submitted to dmzap_target. */
static int dmzap_map(struct dm_target *ti, struct bio *bio)
{
	struct dmzap_target *dmzap = ti->private;
	struct dmz_dev *dev = dmzap->dev;
    /* Sector to be read or written. The last pending sector in the last pending bio vector.*/
	sector_t sector = bio->bi_iter.bi_sector;
    /* Number of sectors involved in the bio. */
	unsigned int nr_sectors = bio_sectors(bio);
	sector_t chunk_sector;
	int ret;

    /* If block device is being removed, fail the bio. */
	if (dmzap_bdev_is_dying(dmzap->dev))
		return DM_MAPIO_KILL;

    /* Debug message to print the details of incoming bio. */
	if(dmzap->show_debug_msg){
		dmz_dev_debug(dev, "BIO op %d sector %llu + %u => chunk %llu, block %llu, %u blocks",
						bio_op(bio), (unsigned long long)sector, nr_sectors,
						(unsigned long long)dmzap_bio_chunk(dev, bio),
						(unsigned long long)dmzap_chunk_block(dev, dmz_bio_block(bio)),
						(unsigned int)dmz_bio_blocks(bio));
	}

    /* Sets block device of the bio, and refreshs its blkg association. */
	bio_set_dev(bio, dev->bdev);

	if (!nr_sectors && bio_op(bio) != REQ_OP_WRITE)
		return DM_MAPIO_REMAPPED;

	/**
     * dmzoned blocks are always 4KB in size, while sectors in bio requests are 512 byte 
     * addressable. Here it checks for miss-alignments and fails the bio if it finds so.
     */
	if ((nr_sectors & DMZ_BLOCK_SECTORS_MASK) || (sector & DMZ_BLOCK_SECTORS_MASK))
		return DM_MAPIO_KILL;

	/* Initializes the bio context. */
	dmzap_init_bioctx(dmzap,bio);

	/* If this bio_vec for write operation is completed, flush the results. */
	if (!nr_sectors && bio_op(bio) == REQ_OP_WRITE) {
		spin_lock(&dmzap->flush_lock);
		bio_list_add(&dmzap->flush_list, bio);
		spin_unlock(&dmzap->flush_lock);
		mod_delayed_work(dmzap->flush_wq, &dmzap->flush_work, 0);
		return DM_MAPIO_SUBMITTED;
	}

	/** 
     * If current bio spans multiple chunks (a chunk is user-side bundle of sectors with size
     * equivalent to the size of a zone), only process the bio to the end of the chunk. The
     * rest of the bio has to be sent in next bios.
     */
	chunk_sector = sector & (dev->zone_nr_sectors - 1);
	if (chunk_sector + nr_sectors > dev->zone_nr_sectors)
		dm_accept_partial_bio(bio, dev->zone_nr_sectors - chunk_sector);

	/* Passes the bio to the chunk work queue to be processed. */
	ret = dmzap_queue_chunk_work(dmzap, bio);
	if (ret) {
		dmz_dev_debug(dev,
			      "BIO op %d, can't process chunk %llu, err %i\n",
			      bio_op(bio), (u64)dmzap_bio_chunk(dev, bio),
			      ret);
		return DM_MAPIO_REQUEUE;
	}

	return DM_MAPIO_SUBMITTED;
}

/**
 * Fetches the information of the dm_dev backing device and uses that information to initialize
 * dmz_dev (dmzap uses dmzoned under the hood).
 */
static int dmzap_get_zoned_device(struct dm_target *ti, char *path)
{
	struct dmzap_target *dmz = ti->private;
	struct request_queue *q;
	struct dmz_dev *dev;
	sector_t aligned_capacity;
	int ret;

	/* Gets information for dm_dev backing device and stores it in dmz->ddev. */
	ret = dm_get_device(ti, path, dm_table_get_mode(ti->table), &dmz->ddev);
	if (ret) {
		ti->error = "Get target device failed";
		dmz->ddev = NULL;
		return ret;
	}

    /* Allocates memory for the internal dmzoned device. */
	dev = kzalloc(sizeof(struct dmz_dev), GFP_KERNEL);
	if (!dev) {
		ret = -ENOMEM;
		goto err;
	}

    /* Sets block device of dmzoned device. */
	dev->bdev = dmz->ddev->bdev;
    /* Sets block device name of dmzoned device. */
	(void)bdevname(dev->bdev, dev->name);

    /* If the used block device is not zoned, return an error. */
	if (bdev_zoned_model(dev->bdev) == BLK_ZONED_NONE) {
		ti->error = "Not a zoned block device";
		ret = -EINVAL;
		goto err;
	}

    /* Gets request queue of the block device. */
	q = bdev_get_queue(dev->bdev);
    /* Gets the number of sectors in the block device. */
	dev->capacity = i_size_read(dev->bdev->bd_inode) >> SECTOR_SHIFT;
    /**
     * Gets the number of sectors in the block device, minus the number of sectors that are
     * outside of the last zone complete (it discounts the partial zone in the end of the 
     * device).
     */
	aligned_capacity = dev->capacity & ~(blk_queue_zone_sectors(q) - 1);
    /**
     * If the underlying dm does not start from the first sector, or its capacity is not
     * equal to the full capacity (or the aligned full capacity) of the block device, fail
     * the process.
     */
	if (ti->begin ||
	    ((ti->len != dev->capacity) && (ti->len != aligned_capacity))) {
		ti->error = "Partial mapping not supported";
		ret = -EINVAL;
		goto err;
	}

    /* Sets the number of sectors in each zone attribute in the block device. */
	dev->zone_nr_sectors = blk_queue_zone_sectors(q);
	// dev->zone_nr_sectors_shift = ilog2(dev->zone_nr_sectors); // dmz_dev no longer has this, it moved to dmz_metadata

	// dev->zone_nr_blocks = dmz_sect2blk(dev->zone_nr_sectors); // this also moved to dmz_metadata
	// dev->zone_nr_blocks_shift = ilog2(dev->zone_nr_blocks); // also moved to dmz_metadata

    /* Sets the number zones attribute in the block device. */
	dev->nr_zones = blkdev_nr_zones(dev->bdev->bd_disk);

    /* Stores the block device pointer in dmzap taget. */
	dmz->dev = dev;

	return 0;
err:
	dm_put_device(ti, dmz->ddev);
	kfree(dev);

	return ret;
}

/*
 * Cleanup zap device information.
 */
static void dmzap_put_zoned_device(struct dm_target *ti)
{
	struct dmzap_target *dmzap = ti->private;

	dm_put_device(ti, dmzap->ddev);
	kfree(dmzap->dev);
	dmzap->dev = NULL;
}

/**
 * ctr handle for dmzap_type - Constructor for dmzap_target. 
 */
static int dmzap_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct dmzap_target *dmzap;
	struct dmz_dev *dev;
	unsigned int nr_conv_zones;
	unsigned int op_rate;
	unsigned int class_0_cap;
	unsigned int class_0_optimal;
	unsigned int victim_selection_method;
	unsigned int reclaim_limit;
	unsigned int q_cap;
	char dummy;
	int ret;
	int error;


	/* We reuse macros and other stuff from dm-zoned.
	 * Make sure that dm-zoned does not unexpectedly change
	 * block size.
	 */
	BUILD_BUG_ON(DMZ_BLOCK_SIZE != 4096);

	/* Validates the number of input arguemnts. */
	if (argc != 8) {
		ti->error = "invalid argument count";
		return -EINVAL;
	}

    /* Validates the number of conventional zones for the dmzap, which has to be 0. */
	if (sscanf(argv[1], "%u%c", &nr_conv_zones, &dummy) != 1 || nr_conv_zones > 0) {
		ti->error = "Invalid number of conventional zones. No conventional zones allowed.";
		return -EINVAL;
	}

    /* Validates the percentage of overprovisioning zones. */
	if (sscanf(argv[2], "%u%c", &op_rate, &dummy) != 1
			|| op_rate > 100 ) {
		ti->error = "Invalid overprovisioning rate";
		return -EINVAL;
	}

    /* Validates the class 0 cap. */
	if (sscanf(argv[3], "%u%c", &class_0_cap, &dummy) != 1) {
		ti->error = "Invalid class 0 cap";
		return -EINVAL;
	}

    /* Validates the class 0 optimal. Has to be less than or equal to class 0 cap. */
	if (sscanf(argv[4], "%u%c", &class_0_optimal, &dummy) != 1
			|| class_0_cap < class_0_optimal ) {
		ti->error = "Invalid class 0 optimal";
		return -EINVAL;
	}

    /**
     * Validates the victim selection method. Has to be within the number of available victim
     * selection methods. 
     */
	if (sscanf(argv[5], "%u%c", &victim_selection_method, &dummy) != 1
			|| victim_selection_method > DMZAP_VICTIM_POLICY_MAX ) {
		ti->error = "Invalid victim selection method";
		return -EINVAL;
	}

    /* Validates the reclaim limit. */
	if (sscanf(argv[6], "%u%c", &reclaim_limit, &dummy) != 1
			|| reclaim_limit > 100 ) {
		ti->error = "Invalid reclaim limit";
		return -EINVAL;
	}

    /* Validates the q limit. */
	if (sscanf(argv[7], "%u%c", &q_cap, &dummy) != 1) {
		ti->error = "Invalid q limit";
		return -EINVAL;
	}

	/* Allocates memory for dmzap target. */
	dmzap = kzalloc(sizeof(struct dmzap_target), GFP_KERNEL);
	if (!dmzap) {
		ti->error = "unable to allocate the zoned target descriptor";
		return -ENOMEM;
	}
	ti->private = dmzap;

    /* Sets overprovisioning rate of dmzap target. */
	dmzap->overprovisioning_rate = op_rate;
    /* Sets class 0 cap of dmzap target. */
	dmzap->class_0_cap = class_0_cap;
    /* Sets class 0 optimal of dmzap target. */
	dmzap->class_0_optimal = class_0_optimal;
    /* Sets victim selection method of dmzap target. */
	dmzap->victim_selection_method = victim_selection_method;
    /* Sets reclaim limit of dmzap target. */
	dmzap->reclaim_limit = reclaim_limit;
    /* Sets q cap of dmzap target. */
	dmzap->q_cap = q_cap;

	/* Initializes dm_dev backing device, and dmz_dev. */
	ret = dmzap_get_zoned_device(ti, argv[0]);
	if (ret) {
		dmzap->ddev = NULL;
		goto err;
	}

    /* Initializes the geometery fields of dmzap_target. */
	ret = dmzap_geometry_init(ti);
	if (ret) {
		goto err_dev;
	}

	dev = dmzap->dev;

    /* Initializes ths zone fields of dmzap_target. */
	ret = dmzap_zones_init(dmzap);
	if (ret) {
		ti->error = "failed to initialize zones";
		goto err_dev;
	}

	spin_lock_init(&dmzap->meta_blk_lock);

    /* Initializes the mapping fields of dmzap_target. */
	ret = dmzap_map_init(dmzap);
	if (ret) {
		ti->error = "failed to initialize mapping table";
		goto err_zones;
	}

	/* set target (no write same support) */
	ti->max_io_len = dev->zone_nr_sectors << 9;
	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	ti->num_write_zeroes_bios = 1;
	ti->per_io_data_size = sizeof(struct dmzap_bioctx);
	ti->flush_supported = true;
	ti->discards_supported = true;

	ti->len = dmzap->capacity;

    /* Initializes the bioset of dmzap_target. */
	ret = bioset_init(&dmzap->bio_set, DMZAP_MIN_BIOS, 0, 0);
	if (ret) {
		ti->error = "create bio set failed";
		goto err_map;
	}

	/* Initializes the reclaim of dmzap_target. */
	ret = dmzap_ctr_reclaim(dmzap);
	if (ret) {
		ti->error = "Zone reclaim initialization failed";
		goto err_map;
	}

	mutex_init(&dmzap->chunk_lock);
    /* Initializes the chunk work radix tree. */
	INIT_RADIX_TREE(&dmzap->chunk_rxtree, GFP_NOIO);
    /* Initializes the chunk work queue. */
	dmzap->chunk_wq = alloc_workqueue("dmzap_cwq_%s", WQ_MEM_RECLAIM | WQ_UNBOUND,
					0, dev->name);
	if (!dmzap->chunk_wq) {
		ti->error = "Create chunk workqueue failed";
		ret = -ENOMEM;
		goto err_bio;
	}

	spin_lock_init(&dmzap->flush_lock);
    /* Initializes the flush bio list */
	bio_list_init(&dmzap->flush_list);
    /* Initializes the flush work. */
	INIT_DELAYED_WORK(&dmzap->flush_work, dmzap_flush_work);
    /* Initializes the flush work queue. */
	dmzap->flush_wq = alloc_ordered_workqueue("dmzap_fwq_%s", WQ_MEM_RECLAIM,
						dev->name);
	if (!dmzap->flush_wq) {
		ti->error = "Create flush workqueue failed";
		ret = -ENOMEM;
		goto err_cwq;
	}
    /* Schedules the flush work on flush work queue. */
	mod_delayed_work(dmzap->flush_wq, &dmzap->flush_work, DMZAP_FLUSH_PERIOD);

	/* Just for debugging purpose TODO REMOVE */
	dmzap->show_debug_msg = 0;
    /* Initializes the number of user-written sectors. */
	dmzap->nr_user_written_sec = 0;
    /* Initializes the number of gc-written sectors. */
	dmzap->nr_gc_written_sec = 0;
    /* Initializes the last time statistics were printed. */
	dmzap->wa_print_time = jiffies;
	dmzap->gc_time = 0;
	dmzap->gc_count = 0;


	dmz_dev_info(dev, "target internal zones: %u meta, %u overprovisioning",
			dmzap->nr_meta_zones, dmzap->nr_op_zones);

	dmz_dev_info(dev, "target device: user zones: %u",
			dmzap->nr_user_exposed_zones);

	dmz_dev_info(dev, "target device: %llu 512-byte logical sectors (%llu blocks)",
		     (unsigned long long)ti->len,
		     (unsigned long long)dmz_sect2blk(ti->len));

	dmz_dev_info(dev, "Write pointer position: %llu",
			(unsigned long long)dmzap_get_seq_wp(dmzap));

	dmz_dev_info(dev, "Victim selection method: %d",
			dmzap->victim_selection_method);

	dmz_dev_info(dev, "Reclaim limit: %d",
			dmzap->reclaim_limit);

	dmz_dev_info(dev, "q_cap: %d",
			dmzap->q_cap);

	// Information for thesis evaluation
	trace_printk("Target setup: internal zones: %u, user exposed zones: %u, op zones: %u\n",
	dmzap->nr_internal_zones,
	dmzap->nr_user_exposed_zones,
	dmzap->nr_op_zones);

	trace_printk("Op rate: %u, class 0 cap: %u, class 0 optimal: %u, reclaim limit: %u\n",
	dmzap->overprovisioning_rate,
	dmzap->class_0_cap,
	dmzap->class_0_optimal,
	dmzap->reclaim_limit);

	trace_printk("Victim selection method: %u, capacity: %lld, dev_capacity: %lld, q_cap: %u\n",
	dmzap->victim_selection_method,
	dmzap->capacity,
	dmzap->dev_capacity,
	dmzap->q_cap);

	zap_stat_kobject = kobject_create_and_add("zap",
                                                 kernel_kobj);
	
	dmzap_ptr = dmzap;
        if(!zap_stat_kobject)
                return -ENOMEM;

        error = sysfs_create_file(zap_stat_kobject, &zap_reset_attribute.attr);
        if (error) {
                pr_debug("failed to create the reset_stats file in /sys/kernel/zap \n");
        }

	mutex_init(&heap_increase_lock);

	return 0;

err_cwq:
	destroy_workqueue(dmzap->chunk_wq);
err_bio:
	mutex_destroy(&dmzap->chunk_lock);
err_map:
	bioset_exit(&dmzap->bio_set);
	dmzap_map_free(dmzap);
err_zones:
	dmzap_zones_free(dmzap);
err_dev:
	dmzap_put_zoned_device(ti);
err:
	kfree(dmzap);

	kobject_put(zap_stat_kobject);

	return ret;
}

/**
 * dtr handle for dmzap_type - Destructor for dmzap_target. 
 */
static void dmzap_dtr(struct dm_target *ti)
{
	struct dmzap_target *dmzap = ti->private;
	flush_workqueue(dmzap->chunk_wq);
	destroy_workqueue(dmzap->chunk_wq);
	dmzap_dtr_reclaim(dmzap);
	cancel_delayed_work_sync(&dmzap->flush_work);
	destroy_workqueue(dmzap->flush_wq);
	bioset_exit(&dmzap->bio_set);
	dmzap_zones_free(dmzap);
	dmzap_map_free(dmzap);
	dmzap_put_zoned_device(ti);
	mutex_destroy(&dmzap->chunk_lock);
	kobject_put(zap_stat_kobject);
	kfree(dmzap);
}


/**
 * io_hints handle for dmzap_type - Outlines the limits of the target request queue.
 */
static void dmzap_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	struct dmzap_target *dmzap = ti->private;
	unsigned int zone_sectors = dmzap->dev->zone_nr_sectors;

	limits->logical_block_size = DMZ_BLOCK_SIZE;
	limits->physical_block_size = DMZ_BLOCK_SIZE;

	blk_limits_io_min(limits, DMZ_BLOCK_SIZE);
	blk_limits_io_opt(limits, DMZ_BLOCK_SIZE);

	limits->discard_alignment = DMZ_BLOCK_SIZE;
	limits->discard_granularity = DMZ_BLOCK_SIZE;
	limits->max_discard_sectors = zone_sectors;
	limits->max_hw_discard_sectors = zone_sectors;
	limits->max_write_zeroes_sectors = zone_sectors;

	/* FS hint to try to align to the device zone size */
	limits->chunk_sectors = zone_sectors;
	limits->max_sectors = zone_sectors;

	/* We are exposing a host-managed zoned block device */
	limits->zoned = BLK_ZONED_HM;
}

/**
 * prepare_ioctl handle for dmzap_type - Handles an ioctl call.
 */
static int dmzap_prepare_ioctl(struct dm_target *ti, struct block_device **bdev)
{
	struct dmzap_target *dmzap = ti->private;

    /* Error if the backing device was dying. */
	if (!dmzap_check_bdev(dmzap->dev))
		return -EIO;

	/* TODO: do we really want to just pipe things through? */
	*bdev = dmzap->dev->bdev;

	return 0;
}

/**
 * postsuspend handle for dmzap_type - Stops background works.
 */
static void dmzap_suspend(struct dm_target *ti)
{
	struct dmzap_target *dmzap = ti->private;

    /* Waits for all chunk works to finish. */
	flush_workqueue(dmzap->chunk_wq);
    /* Suspends reclaim. */
	dmzap_suspend_reclaim(dmzap);
    /* Drops the scheduled flush work. */
	cancel_delayed_work_sync(&dmzap->flush_work);
}

/**
 * resume handle for dmzap_type -  Resume background works.
 */
static void dmzap_resume(struct dm_target *ti)
{
	struct dmzap_target *dmzap = ti->private;

    /* Resumes flush work.*/
	queue_delayed_work(dmzap->flush_wq, &dmzap->flush_work, DMZAP_FLUSH_PERIOD);
    /* Resumes reclaim.*/
	dmzap_resume_reclaim(dmzap);
}

/**
 * iterate_devices handle for dmzap_type -  Resume background works.
 */
/* Passes device mapper information to the callback. */
static int dmzap_iterate_devices(struct dm_target *ti,
				 iterate_devices_callout_fn fn, void *data)
{
	struct dmzap_target *dmzap = ti->private;
	struct dmz_dev *dev = dmzap->dev;
	sector_t capacity = dev->capacity;

	/*  TODO: validate this */
	return fn(ti, dmzap->ddev, 0, capacity, data);
}

static struct target_type dmzap_type = {
	.name		 = "zap",
	.version	 = {1, 0, 0},
	.features	 = DM_TARGET_ZONED_HM,
	.module		 = THIS_MODULE,
	.report_zones	 = dmzap_report_zones,
	.ctr		 = dmzap_ctr,
	.dtr		 = dmzap_dtr,
	.map		 = dmzap_map,
	.io_hints	 = dmzap_io_hints,
	.prepare_ioctl	 = dmzap_prepare_ioctl,
	.postsuspend	 = dmzap_suspend,
	.resume		 = dmzap_resume,
	.iterate_devices = dmzap_iterate_devices,
};

static int __init dmzap_init(void)
{
	return dm_register_target(&dmzap_type);
}

static void __exit dmzap_exit(void)
{
	dm_unregister_target(&dmzap_type);
}

module_init(dmzap_init);
module_exit(dmzap_exit);

MODULE_DESCRIPTION(DM_NAME " target");
MODULE_AUTHOR("Hans Holmberg <hans.holmberg@wdc.com>");
MODULE_LICENSE("GPL");