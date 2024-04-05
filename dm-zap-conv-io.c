// SPDX-License-Identifier: GPL-2.0
/*
 * Copyright (C) 2019 Western Digital Corporation or its affiliates.
 *
 */

#include "dm-zap.h"


/*
 *	Updates the current zones wp and if nessesary the dmzap_zone_wp
 */
void dmzap_update_seq_wp(struct dmzap_target *dmzap, sector_t bio_sectors)
{
	u32 i = 0;
	u32 current_zone = 0;
	struct blk_zone	*zone = dmzap->dmzap_zones[dmzap->dmzap_zone_wp].zone;

	zone->wp += bio_sectors;
	zone->cond = BLK_ZONE_COND_IMP_OPEN;

	if (zone->wp >= zone->start + zone->len) { //TODO ZNS capacity: if (zone->wp >= zone->start + zone->capacity) {
		//TODO figure out how to finish the zone.
		// ret = blkdev_zone_mgmt(dmzap->dev->bdev, REQ_OP_ZONE_FINISH,
		//  					 zone->start, dmzap->dev->zone_nr_sectors, GFP_NOIO);
		// if(ret){
		// 	dmz_dev_err(dmzap->dev, "Zone finish failed! Return value: %d", ret);
		// 	return;
		// }
		zone->cond = BLK_ZONE_COND_FULL;
		dmzap->reclaim->nr_free_zones--;
		dmzap_calc_p_free_zone(dmzap);
		trace_printk("-Number of free zones %lu. (total %u)\n", dmzap->reclaim->nr_free_zones, dmzap->nr_internal_zones);

		if(dmzap->victim_selection_method == DMZAP_FAST_CB){
			dmzap_assign_zone_to_reclaim_class(dmzap, &dmzap->dmzap_zones[dmzap->dmzap_zone_wp]);
		}

		if(dmzap->victim_selection_method == DMZAP_CONST_CB || dmzap->victim_selection_method == DMZAP_CONST_GREEDY){
			list_add_tail(&(dmzap->dmzap_zones[dmzap->dmzap_zone_wp].num_invalid_blocks_link), &(dmzap->num_invalid_blocks_lists[dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks]));
			//dmz_dev_debug(dmzap->dev, "Added zone %u to list %d for cb", dmzap->dmzap_zone_wp, dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks);
		}

		if(dmzap->victim_selection_method == DMZAP_FEGC){
			// for (j=0;j < dmzap->dev->zone_nr_blocks;j++){
			//	 sum += dmzap->fegc_heaps[j].size;
			// }
			// dmzap->dmzap_zones[dmzap->dmzap_zone_wp].cwa = 0;
			// dmzap->dmzap_zones[dmzap->dmzap_zone_wp].cwa_time = 0;
			// if (dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks > 256 || dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks < 0){
			// 	dmz_dev_info(dmzap->dev, "dmzap->dmzap_zone_wp: %u, mzap->dmzap_zones[dmzap->dmzap_zone_wp]: %u, invalids: %d", dmzap->dmzap_zone_wp, dmzap->dmzap_zones[dmzap->dmzap_zone_wp].seq, dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks);
			// }
			dmzap_heap_insert(dmzap->fegc_heaps[dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks], &dmzap->dmzap_zones[dmzap->dmzap_zone_wp]);
			//dmz_dev_info(dmzap->dev, "After inserting zone %u to heap %d in wp", dmzap->dmzap_zones[dmzap->dmzap_zone_wp].seq, dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks);
			// if (!heaps_are_ok(dmzap)){
				
			// 	heap_print(dmzap, dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks);
			// 	BUG();
			// }
			// assert_heap_ok(dmzap, 1);
			//dmz_dev_debug(dmzap->dev, "Added zone %u to heap %px for FeGC", dmzap->dmzap_zone_wp, &dmzap->fegc_heaps[dmzap->dmzap_zones[dmzap->dmzap_zone_wp].nr_invalid_blocks]);

		}
		if(dmzap->victim_selection_method == DMZAP_FAGCPLUS){
			dmzap_heap_insert(&dmzap->fagc_heap, &dmzap->dmzap_zones[dmzap->dmzap_zone_wp]);
		}
		
		/* Find new zone to write to*/
		for (i = 1; i < dmzap->nr_internal_zones; i++) {
			current_zone = (i + dmzap->dmzap_zone_wp) % dmzap->nr_internal_zones;
			zone = dmzap->dmzap_zones[current_zone].zone;
			if (zone->cond < BLK_ZONE_COND_CLOSED
				&& dmzap->dmzap_zones[current_zone].type == DMZAP_RAND) {
				dmzap->dmzap_zone_wp = dmzap->dmzap_zones[current_zone].seq;
				dmzap->dmzap_zones[dmzap->dmzap_zone_wp].state = DMZAP_OPENED;
				dmzap->nr_clean_zones--;
				dmzap->nr_opened_zones++;
				return;
			}
		}
		dmz_dev_info(dmzap->dev, "Device is completely full.\n");
	}
}

static inline void dmzap_bio_end_wr(struct bio *bio,
	blk_status_t status)
{
	struct dmzap_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmzap_bioctx));
	struct dmzap_target *dmzap = bioctx->target;

    /* Alarms if write bio fails. */
	if (bio->bi_status != BLK_STS_OK) {
		/* TODO: stop writing to zone
		 *  (or writing altogether) on
		 *  failed writes
		 */
		dmz_dev_err(dmzap->dev, "write failed! bi_status %d", bio->bi_status);
	} else {
		int ret;

        /* Updates the mapping table. */
		ret = dmzap_map_update(dmzap,
			dmz_sect2blk(bioctx->user_sec),
			dmz_sect2blk(dmzap_get_seq_wp(dmzap)),
			dmz_bio_blocks(bio));

        /* Updates zone write pointer (zone to be written).*/
		dmzap_update_seq_wp(dmzap, bio_sectors(bio));

		if(ret)
			dmz_dev_err(dmzap->dev, "endio mapping failed!");
	}

    /* Frees the write outstanding lock. */
	clear_bit_unlock(DMZAP_WR_OUTSTANDING, &dmzap->write_bitmap);
}

/* Increases the reference count of the bio. */
static inline void dmzap_get_bio(struct bio *bio)
{
	struct dmzap_bioctx *bioctx;

	bioctx = dm_per_bio_data(bio, sizeof(struct dmzap_bioctx));
	refcount_inc(&bioctx->ref);
}

/* Decreases the reference count of the bio, and deletes it if it reachs zero. */
static inline void dmzap_put_bio(struct bio *bio)
{
	struct dmzap_bioctx *bioctx;

	bioctx = dm_per_bio_data(bio, sizeof(struct dmzap_bioctx));

	if (refcount_dec_and_test(&bioctx->ref))
		bio_endio(bio);
}

/*
 * Completion callback for an internally cloned target bio. This terminates the
 * target bio when there are no more references to its context.
 */
static void dmzap_clone_endio(struct bio *clone)
{
	struct dmzap_bioctx *bioctx = clone->bi_private;
	struct bio *bio = bioctx->bio;
	blk_status_t status = clone->bi_status;

	// if(status != 0){
	// 	printk("status in dmzap_clone_endio %d", status);
	// }

    /* If the clone failed, fail the original as well. */
	if (status != BLK_STS_OK && bio->bi_status == BLK_STS_OK)
		bio->bi_status = status;

    /* If the bio was write, update the mapping table and zone write pointers. */
	if (bio_data_dir(bio) == WRITE)
		dmzap_bio_end_wr(bio, status);

    /* Decreases the reference counts and deletes the bio and the clone if necessary. */
	bio_put(clone);
	dmzap_bio_endio(bio, status);
}

/**
 * Issues a clone of the target bio and submits it. The clone may only partially process the
 * original target bio.
 */
static int dmzap_submit_bio(struct dmzap_target *dmzap,
				sector_t sector, struct bio *bio)
{
	struct dmzap_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmzap_bioctx));
	struct bio *clone;

    /* Clones the bio, but reuses the bi_vecs of the original. */
	clone = bio_clone_fast(bio, GFP_NOIO, &dmzap->bio_set);
	if (!clone)
		return -ENOMEM;

    /* Sets block device of the clone. */
	bio_set_dev(clone, dmzap->dev->bdev);

    /* Sets bio sector of the clone. */
	clone->bi_iter.bi_sector = sector;
    /* Sets bio size of the clone. */
	clone->bi_iter.bi_size = bio_sectors(bio) << SECTOR_SHIFT;
    /* Sets end io callback of the clone. */
	clone->bi_end_io = dmzap_clone_endio;
    /* Sets private bio context of the clone. */
	clone->bi_private = bioctx;

	//bio_advance(bio, clone->bi_iter.bi_size);

    /* Increments reference count of the bio context, and submits the clone. */
	refcount_inc(&bioctx->ref);
	submit_bio_noacct(clone);

	switch (bio_op(bio)) {
	case REQ_OP_READ:
		//maybe this section is not needed
		break;
	case REQ_OP_WRITE:
        /* Updates the last access time and condition of the zone which was accessed. */
		dmzap->dmzap_zones[dmzap->dmzap_zone_wp].zone_age = jiffies;
		dmzap->dmzap_zones[dmzap->dmzap_zone_wp].zone->cond = BLK_ZONE_COND_IMP_OPEN;
		break;
	}

	return 0;
}

/* Handles conventional read operation. */
int dmzap_conv_read(struct dmzap_target *dmzap, struct bio *bio)
{
	sector_t user = bio->bi_iter.bi_sector;
	unsigned int size;
	sector_t backing;
	int mapped;
	sector_t left = dmz_bio_blocks(bio);
	int ret;

    /* While there is unread block remaining. */
	while (left) {
        /* Gets a segment of contiguously mapped logical blocks. */
		mapped = dmzap_map_lookup(dmzap,
				dmz_sect2blk(user),
				&backing, left);

        /* Converts number of blocks to number of sectors. */
		size = mapped << DMZ_BLOCK_SHIFT;

        /* If the backing block in unmapped or invalid, fills the whole bio with zero.*/
		if (backing == DMZAP_UNMAPPED || backing == DMZAP_INVALID) {

			swap(bio->bi_iter.bi_size, size);
			zero_fill_bio(bio);
			swap(bio->bi_iter.bi_size, size);

        /* Otherwise, submits the read bio for that contiguous size. */
		} else {
			//dmzap_get_bio(bio);

			ret = dmzap_submit_bio(dmzap, dmz_blk2sect(backing), bio);
			if (ret)
				return DM_MAPIO_KILL;
		}

        /* Advances the bio to eliminate the submitted portion. */
		bio_advance(bio, size);
		left -= mapped;
	}

	//dmzap_put_bio(bio);

	return DM_MAPIO_SUBMITTED;
}

/* Handles conventional write operation. */
int dmzap_conv_write(struct dmzap_target *dmzap, struct bio *bio)
{
	int ret;

	/* Only one outstanding write at any time is permitted. Locks the write outstanding. */
	while(test_and_set_bit_lock(DMZAP_WR_OUTSTANDING,
				&dmzap->write_bitmap))
		io_schedule();

    /* If the backing zone to be written is readonly, fails. */
	if (dmzap->dmzap_zones[dmzap->dmzap_zone_wp].zone->cond == BLK_ZONE_COND_READONLY)
		return -EROFS;

    /* Maps the bio to the sequential write pointer. */
	ret = dmzap_submit_bio(dmzap, dmzap_get_seq_wp(dmzap), bio);
	if (ret) {
		/* Out of memory, try again later */
		clear_bit_unlock(DMZAP_WR_OUTSTANDING, &dmzap->write_bitmap);
		return ret;
	}

	return DM_MAPIO_SUBMITTED;
}

/* Handles discard operation. */
int dmzap_handle_discard(struct dmzap_target *dmzap, struct bio *bio)
{
	int ret = DM_MAPIO_SUBMITTED;
	//TODO: implement
	return ret;
}