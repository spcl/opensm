/*
 * Copyright (c) 2004-2008 Voltaire, Inc. All rights reserved.
 * Copyright (c) 2002-2015 Mellanox Technologies LTD. All rights reserved.
 * Copyright (c) 1996-2003 Intel Corporation. All rights reserved.
 * Copyright (c) 2009-2015 ZIH, TU Dresden, Federal Republic of Germany. All rights reserved.
 * Copyright (C) 2012-2017 Tokyo Institute of Technology. All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

#ifndef OSM_UCAST_DFSSSP_H
#define OSM_UCAST_DFSSSP_H
/* "infinity" for dijkstra */
#define INF      0x7FFFFFFF

enum {
	UNDISCOVERED = 0,
	DISCOVERED
};

enum {
	UNKNOWN = 0,
	GRAY,
	BLACK,
};

typedef struct link {
	uint64_t guid;		/* guid of the neighbor behind the link */
	uint32_t from;		/* base_index in the adjazenz list (start of the link) */
	uint8_t from_port;	/* port on the base_side (needed for weight update to identify the correct link for multigraphs) */
	uint32_t to;		/* index of the neighbor in the adjazenz list (end of the link) */
	uint8_t to_port;	/* port on the side of the neighbor (needed for the LFT) */
	uint64_t weight;	/* link weight */
	struct link *next;
} link_t;

typedef struct vertex {
	/* informations of the fabric */
	uint64_t guid;
	uint16_t lid;		/* for lft filling */
	uint32_t num_hca;	/* numbers of Hca/LIDs on the switch, for weight calculation */
	link_t *links;
	uint8_t hops;
	/* for dijkstra routing */
	link_t *used_link;	/* link between the vertex discovered before and this vertex */
	uint64_t distance;	/* distance from source to this vertex */
	uint8_t state;
	/* for the d-ary heap */
	size_t heap_index;
	/* for LFT writing and debug */
	osm_switch_t *sw;	/* selfpointer */
	boolean_t dropped;	/* indicate dropped switches (w/ ucast cache) */
} vertex_t;

typedef struct vltable {
	uint64_t num_lids;	/* size of the lids array */
	uint16_t *lids;		/* sorted array of all lids in the subnet */
	uint8_t *vls;		/* matrix form assignment lid X lid -> virtual lane */
} vltable_t;

typedef struct cdg_link {
	struct cdg_node *node;
	uint32_t num_pairs;	/* number of src->dest pairs incremented in path adding step */
	uint32_t max_len;	/* length of the srcdest array */
	uint32_t removed;	/* number of pairs removed in path deletion step */
	uint32_t *srcdest_pairs;
	struct cdg_link *next;
} cdg_link_t;

/* struct for a node of a binary tree with additional parent pointer */
typedef struct cdg_node {
	uint64_t channelID;	/* unique key consist of src lid + port + dest lid + port */
	cdg_link_t *linklist;	/* edges to adjazent nodes */
	uint8_t status;		/* node status in cycle search to avoid recursive function */
	uint8_t visited;	/* needed to traverse the binary tree */
	struct cdg_node *pre;	/* to save the path in cycle detection algorithm */
	struct cdg_node *left, *right, *parent;
} cdg_node_t;

typedef struct dfsssp_context {
	osm_routing_engine_type_t routing_type;
	osm_ucast_mgr_t *p_mgr;
	vertex_t *adj_list;
	uint32_t adj_list_size;
	vltable_t *srcdest2vl_table;
	uint8_t *vl_split_count;
} dfsssp_context_t;


/**************** set initial values for structs **********************
 **********************************************************************/
static inline void set_default_link(link_t * link)
{
	link->guid = 0;
	link->from = 0;
	link->from_port = 0;
	link->to = 0;
	link->to_port = 0;
	link->weight = 0;
	link->next = NULL;
}

static inline void set_default_vertex(vertex_t * vertex)
{
	vertex->guid = 0;
	vertex->lid = 0;
	vertex->num_hca = 0;
	vertex->links = NULL;
	vertex->hops = 0;
	vertex->used_link = NULL;
	vertex->distance = 0;
	vertex->state = UNDISCOVERED;
	vertex->heap_index = 0;
	vertex->sw = NULL;
	vertex->dropped = FALSE;
}

static inline void set_default_cdg_node(cdg_node_t * node)
{
	node->channelID = 0;
	node->linklist = NULL;
	node->status = UNKNOWN;
	node->visited = 0;
	node->pre = NULL;
	node->left = NULL;
	node->right = NULL;
	node->parent = NULL;
}

/**********************************************************************
 **********************************************************************/

/************ helper functions to save src/dest X vl combination ******
 **********************************************************************/

static void vltable_dealloc(vltable_t ** vltable)
{
	if (*vltable) {
		if ((*vltable)->lids)
			free((*vltable)->lids);
		if ((*vltable)->vls)
			free((*vltable)->vls);
		free(*vltable);
		*vltable = NULL;
	}
}

/**********************************************************************
 **********************************************************************/

/************ helper functions to generate an ordered list of ports ***
 ************ (functions copied from osm_ucast_mgr.c and modified) ****
 **********************************************************************/
static void add_sw_endports_to_order_list(osm_switch_t * sw,
					  osm_ucast_mgr_t * m,
					  cl_qmap_t * guid_tbl,
					  boolean_t add_guids)
{
	osm_port_t *port;
	ib_net64_t port_guid;
	uint64_t sw_guid;
	osm_physp_t *p;
	int i;
	boolean_t found;

	for (i = 1; i < sw->num_ports; i++) {
		p = osm_node_get_physp_ptr(sw->p_node, i);
		if (p && p->p_remote_physp && !p->p_remote_physp->p_node->sw) {
			port_guid = p->p_remote_physp->port_guid;
			/* check if link is healthy, otherwise ignore CA */
			if (!osm_link_is_healthy(p)) {
				sw_guid =
				    cl_ntoh64(osm_node_get_node_guid
					      (sw->p_node));
				OSM_LOG(m->p_log, OSM_LOG_INFO,
					"WRN AD40: ignoring CA due to unhealthy"
					" link from switch 0x%016" PRIx64
					" port %" PRIu8 " to CA 0x%016" PRIx64
					"\n", sw_guid, i, cl_ntoh64(port_guid));
			}
			port = osm_get_port_by_guid(m->p_subn, port_guid);
			if (!port)
				continue;
			if (!cl_is_qmap_empty(guid_tbl)) {
				found = (cl_qmap_get(guid_tbl, port_guid)
					 != cl_qmap_end(guid_tbl));
				if ((add_guids && !found)
				    || (!add_guids && found))
					continue;
			}
			if (!cl_is_item_in_qlist(&m->port_order_list,
						 &port->list_item))
				cl_qlist_insert_tail(&m->port_order_list,
						     &port->list_item);
			else
				OSM_LOG(m->p_log, OSM_LOG_INFO,
					"WRN AD37: guid 0x%016" PRIx64
					" already in list\n", port_guid);
		}
	}
}

static void add_guid_to_order_list(uint64_t guid, osm_ucast_mgr_t * m)
{
	osm_port_t *port = osm_get_port_by_guid(m->p_subn, cl_hton64(guid));

	if (!port) {
		 OSM_LOG(m->p_log, OSM_LOG_DEBUG,
			 "port guid not found: 0x%016" PRIx64 "\n", guid);
	}

	if (!cl_is_item_in_qlist(&m->port_order_list, &port->list_item))
		cl_qlist_insert_tail(&m->port_order_list, &port->list_item);
	else
		OSM_LOG(m->p_log, OSM_LOG_INFO,
			"WRN AD38: guid 0x%016" PRIx64 " already in list\n",
			guid);
}

/* compare function of #Hca attached to a switch for stdlib qsort */
static int cmp_num_hca(const void * l1, const void * l2)
{
	vertex_t *sw1 = *((vertex_t **) l1);
	vertex_t *sw2 = *((vertex_t **) l2);
	uint32_t num_hca1 = 0, num_hca2 = 0;

	if (sw1)
		num_hca1 = sw1->num_hca;
	if (sw2)
		num_hca2 = sw2->num_hca;

	if (num_hca1 > num_hca2)
		return -1;
	else if (num_hca1 < num_hca2)
		return 1;
	else
		return 0;
}

/* use stdlib to sort the switch array depending on num_hca */
static inline void sw_list_sort_by_num_hca(vertex_t ** sw_list,
					   uint32_t sw_list_size)
{
	qsort(sw_list, sw_list_size, sizeof(vertex_t *), cmp_num_hca);
}

/**********************************************************************
 **********************************************************************/

/************ helper functions to manage a map of CN and I/O guids ****
 **********************************************************************/
static int add_guid_to_map(void * cxt, uint64_t guid, char * p)
{
	cl_qmap_t *map = cxt;
	name_map_item_t *item;
	name_map_item_t *inserted_item;

	item = malloc(sizeof(*item));
	if (!item)
		return -1;

	item->guid = cl_hton64(guid);	/* internal: network byte order */
	item->name = NULL;		/* name isn't needed */
	inserted_item = (name_map_item_t *) cl_qmap_insert(map, item->guid, &item->item);
	if (inserted_item != item)
                free(item);

	return 0;
}

static void destroy_guid_map(cl_qmap_t * guid_tbl)
{
	name_map_item_t *p_guid = NULL, *p_next_guid = NULL;

	p_next_guid = (name_map_item_t *) cl_qmap_head(guid_tbl);
	while (p_next_guid != (name_map_item_t *) cl_qmap_end(guid_tbl)) {
		p_guid = p_next_guid;
		p_next_guid = (name_map_item_t *) cl_qmap_next(&p_guid->item);
		free(p_guid);
	}
	cl_qmap_remove_all(guid_tbl);
}

/**********************************************************************
 **********************************************************************/

static void dfsssp_print_graph(osm_ucast_mgr_t * p_mgr, vertex_t * adj_list,
			       uint32_t size)
{
	uint32_t i = 0, c = 0;
	link_t *link = NULL;

	/* index 0 is for the source in dijkstra -> ignore */
	for (i = 1; i < size; i++) {
		OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG, "adj_list[%" PRIu32 "]:\n",
			i);
		OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
			"   guid = 0x%" PRIx64 " lid = %" PRIu16 " (%s)\n",
			adj_list[i].guid, adj_list[i].lid,
			adj_list[i].sw->p_node->print_desc);
		OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
			"   num_hca = %" PRIu32 "\n", adj_list[i].num_hca);

		c = 1;
		for (link = adj_list[i].links; link != NULL;
		     link = link->next, c++) {
			OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
				"   link[%" PRIu32 "]:\n", c);
			OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
				"      to guid = 0x%" PRIx64 " (%s) port %"
				PRIu8 "\n", link->guid,
				adj_list[link->to].sw->p_node->print_desc,
				link->to_port);
			OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
				"      weight on this link = %" PRIu64 "\n",
				link->weight);
		}
	}
}

#endif

