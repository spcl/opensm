#if HAVE_CONGIF_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <complib/cl_heap.h>

#include <opensm/osm_file_ids.h>
#define FILE_ID OSM_FILE_UCAST_RUES_C
#include <opensm/osm_ucast_mgr.h>
#include <opensm/osm_opensm.h>
#include <opensm/osm_node.h>
#include <opensm/osm_multicast.h>
#include <opensm/osm_mcast_mgr.h>

#include "opensm/osm_ucast_dfsssp.h"

#include <stdbool.h>

#define max(x,y) ((x) >= (y)) ? (x) : (y)
#define min(x,y) ((x) <= (y)) ? (x) : (y)

typedef struct rues_context {
    osm_routing_engine_type_t routing_type;
    osm_ucast_mgr_t *p_mgr;
    vertex_t *adj_list; // full adj_list
    uint32_t adj_list_size;
    vltable_t *srcdest2vl_table;
    uint8_t *vl_split_count;
    uint8_t number_of_layers;
    uint8_t p; // probability of an edge being included in a layer as a percentage out of 100
    boolean_t ensure_connected;
    boolean_t first_layer_complete;
    boolean_t apply_dfsssp;
} rues_context_t;

static void print_layer(rues_context_t *rues_context, osm_ucast_mgr_t *p_mgr, uint8_t layer_number)
{
    uint32_t i = 0;
    link_t *link = NULL;
    vertex_t *adj_list = rues_context->adj_list;
    uint32_t number_of_layer_entries = rues_context->adj_list_size -1;
    OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG, "Printing layer: %" PRIu8 "\n\n", layer_number);
    for(i = 0; i < number_of_layer_entries; i++) {
        OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG, "adj_list[%" PRIu32 "]:\n",
            i+1);
        OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
            "   guid = 0x%" PRIx64 " lid = %" PRIu16 " (%s)\n",
            adj_list[i+1].guid, adj_list[i+1].lid,
            adj_list[i+1].sw->p_node->print_desc);
        OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
            "   num_hca = %" PRIu32 "\n", adj_list[i+1].num_hca);
        OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
            "Edges included in this layer to other switches:\n");
        link = adj_list[i+1].links;
        while(link) {
            if(link->layer_mapping[layer_number]) {
                OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
                        "    dst_sw_lid: %" PRIu16 " out_port: %" PRIu8 "\n",
                        adj_list[link->to].lid, link->from_port);
            } else {
                OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
                        "    dst_sw_lid: %" PRIu16 "  dst_sw_idx = %" PRIu32 "NO LINK IN THIS LAYER\n", adj_list[link->to].lid, link->to);
            }
	    link = link->next;
        }
    }
}

static rues_context_t *rues_context_create(osm_opensm_t *p_osm, osm_routing_engine_type_t routing_type)
{
    rues_context_t *rues_context = NULL;

    /* allocate memory */
    rues_context = (rues_context_t *) malloc(sizeof(rues_context_t));
    if(rues_context) {
        rues_context->routing_type = routing_type;
        rues_context->p_mgr = (osm_ucast_mgr_t *) & (p_osm->sm.ucast_mgr);
        rues_context->adj_list = NULL;
        rues_context->adj_list_size = 0;
        rues_context->srcdest2vl_table = NULL;
        rues_context->vl_split_count = NULL;

        rues_context->number_of_layers = 1;
        rues_context->p = rues_context->p_mgr->p_subn->opt.rues_prob;
        rues_context->ensure_connected = rues_context->p_mgr->p_subn->opt.rues_connected;
        rues_context->first_layer_complete = rues_context->p_mgr->p_subn->opt.rues_first_complete;
        rues_context->apply_dfsssp = rues_context->p_mgr->p_subn->opt.layers_remove_deadlocks;
    } else {
        OSM_LOG(p_osm->sm.ucast_mgr.p_log, OSM_LOG_ERROR,
                "ERR AD04: cannot allocate memory for rues_context in rues_context_create\n");
        return NULL;
    }

    return rues_context;
}

static void free_adj_list(vertex_t **adj_list, uint32_t adj_list_size) {

    if(*adj_list) {
        uint32_t i = 0;
        link_t *link = NULL, *tmp = NULL;

        /* free adj_list */
        for (i = 0; i < adj_list_size; i++) {
            link = (*adj_list)[i].links;
            while (link) {
                if(link->layer_mapping)
                    free(link->layer_mapping);
                tmp = link;
                link = link->next;
                free(tmp);
            }
        }
        free(*adj_list);
        *adj_list = NULL;
    }
}

static void rues_context_destroy(void *context)
{
    rues_context_t *rues_context = (rues_context_t *) context;
    vertex_t *adj_list = (vertex_t *) (rues_context->adj_list);
    free_adj_list(&adj_list, rues_context->adj_list_size);
    rues_context->adj_list = NULL;
    rues_context->adj_list_size = 0;

    /* free srcdest2vl table and the split count information table
       (can be done, because dfsssp_context_destroy is called after
       osm_get_dfsssp_sl)
       */
    vltable_dealloc(&(rues_context->srcdest2vl_table));
    rues_context->srcdest2vl_table = NULL;

    if (rues_context->vl_split_count) {
        free(rues_context->vl_split_count);
        rues_context->vl_split_count = NULL;
    }
}

static void delete(void *context)
{
    if(!context)
        return;
    rues_context_destroy(context);

    free(context);
}

/* callback function for the cl_heap to update the index */
static void apply_index_update(const void * context, const size_t new_index)
{
    vertex_t *heap_elem = (vertex_t *) context;
    if (heap_elem)
        heap_elem->heap_index = new_index;
}

static int dijkstra(osm_ucast_mgr_t * p_mgr, cl_heap_t * p_heap,
        vertex_t * adj_list, uint32_t adj_list_size,
        osm_port_t * port, uint16_t lid, boolean_t use_link_subset, uint8_t layer_number)
{
    uint32_t i = 0, j = 0, index = 0;
    osm_node_t *remote_node = NULL;
    uint8_t remote_port = 0;
    vertex_t *current = NULL;
    link_t *link = NULL;
    uint64_t guid = 0;
    cl_status_t ret = CL_SUCCESS;

    OSM_LOG_ENTER(p_mgr->p_log);

    /* build an 4-ary heap to find the node with minimum distance */
    // Just a min heap
    /*if (!cl_is_heap_inited(p_heap))
        ret = cl_heap_init(p_heap, adj_list_size, 4,
                &apply_index_update, NULL);
    else
        ret = cl_heap_resize(p_heap, adj_list_size);*/
    ret = cl_heap_init(p_heap, adj_list_size, 4,
            &apply_index_update, NULL);
    if (ret != CL_SUCCESS) {
        OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                "ERR AD09: cannot allocate memory or resize heap\n");
        return ret;
    }

    /* reset all switches for new round with a new source for dijkstra */
    for (i = 1; i < adj_list_size; i++) {
        adj_list[i].hops = 0;
        adj_list[i].used_link = NULL;
        adj_list[i].distance = INF;
        adj_list[i].state = UNDISCOVERED;
        ret = cl_heap_insert(p_heap, INF, &adj_list[i]);
        if (ret != CL_SUCCESS) {
            OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                    "ERR AD11: cl_heap_insert failed\n");
            return ret;
        }
    }

    /* if behind port is a Hca -> set adj_list[0] */
    if (osm_node_get_type(port->p_node) == IB_NODE_TYPE_CA) {
        // if the link behind the port is a HCA, we start by copying basic information into position 0 of adj
        /* save old link to prevent many mallocs after set_default_... */
        link = adj_list[0].links;
        /* initialize adj_list[0] (the source for the routing, a Hca) */
        set_default_vertex(&adj_list[0]);
        adj_list[0].guid =
            cl_ntoh64(osm_node_get_node_guid(port->p_node));
        adj_list[0].lid = lid;
        index = 0;
        /* write saved link back to new adj_list[0] */
        adj_list[0].links = link;

        /* initialize link to neighbor for adj_list[0];
           make sure the link is healthy
           */
        if (port->p_physp && osm_link_is_healthy(port->p_physp)) {
            remote_node =
                osm_node_get_remote_node(port->p_node,
                        port->p_physp->port_num,
                        &remote_port);
            /* if there is no remote node on this port or it's the same Hca -> ignore and we will end up doing nothing*/
            if (remote_node
                    && (osm_node_get_type(remote_node) ==
                        IB_NODE_TYPE_SWITCH)) {
                // If the HCA is connected to a switch we insert a link for the copy of that HCA vertex to that switch and init it with weight 1
                if (!(adj_list[0].links)) {
                    adj_list[0].links =
                        (link_t *) malloc(sizeof(link_t));
                    if (!(adj_list[0].links)) {
                        OSM_LOG(p_mgr->p_log,
                                OSM_LOG_ERROR,
                                "ERR AD07: cannot allocate memory for a link\n");
                        return 1;
                    }
                }
                set_default_link(adj_list[0].links);
                adj_list[0].links->guid =
                    cl_ntoh64(osm_node_get_node_guid
                            (remote_node));
                adj_list[0].links->from_port =
                    port->p_physp->port_num;
                adj_list[0].links->to_port = remote_port;
                adj_list[0].links->weight = 1;
                for (j = 1; j < adj_list_size; j++) {
                    if (adj_list[0].links->guid ==
                            adj_list[j].guid) {
                        adj_list[0].links->to = j;
                        break;
                    }
                }
            }
        } else {
            /* if link is unhealthy then there's a severe issue */
            OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                    "ERR AD0B: unsupported network state (CA with"
                    " unhealthy link state discovered; should have"
                    " been filtered out before already; gracefully"
                    " shutdown this routing engine)\n");
            return 1;
        }
        ret = cl_heap_insert(p_heap, INF, &adj_list[0]);
        if (ret != CL_SUCCESS) {
            OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                    "ERR AD13: cl_heap_insert failed\n");
            return ret;
        }
        /* if behind port is a switch -> search switch in adj_list */
    } else {
        /* reset adj_list[0], if links=NULL reset was done before, then skip */
        if (adj_list[0].links) {
            free(adj_list[0].links);
            set_default_vertex(&adj_list[0]);
        }
        /* search for the switch which is the source in this round */
        guid = cl_ntoh64(osm_node_get_node_guid(port->p_node));
        for (i = 1; i < adj_list_size; i++) {
            if (guid == adj_list[i].guid) {
                index = i;
                break;
            }
        }
    }

    /* source in dijkstra */
    adj_list[index].distance = 0;
    adj_list[index].state = DISCOVERED;
    adj_list[index].hops = 0;	/* the source has hop count = 0 */
    ret = cl_heap_modify_key(p_heap, adj_list[index].distance,
            adj_list[index].heap_index);
    if (ret != CL_SUCCESS) {
        OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                "ERR AD10: index out of bounds in cl_heap_modify_key\n");
        return ret;
    }

    current = (vertex_t *) cl_heap_extract_root(p_heap);
    while (current) {
        current->state = DISCOVERED;
        if (current->used_link)	/* increment the number of hops to the source for each new node */
            current->hops =
                adj_list[current->used_link->from].hops + 1;

        /* add/update nodes which aren't discovered but accessible */
        for (link = current->links; link != NULL; link = link->next) {
            if(use_link_subset && link->layer_mapping && !link->layer_mapping[layer_number])
                continue;
            if ((adj_list[link->to].state != DISCOVERED)
                    && (current->distance + link->weight <
                        adj_list[link->to].distance)) {
                adj_list[link->to].used_link = link;
                adj_list[link->to].distance =
                    current->distance + link->weight;
                ret = cl_heap_modify_key(p_heap,
                        adj_list[link->to].distance,
                        adj_list[link->to].heap_index);
                if (ret != CL_SUCCESS) {
                    OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                            "ERR AD12: index out of bounds in cl_heap_modify_key\n");
                    return ret;
                }
            }
        }

        current = (vertex_t *) cl_heap_extract_root(p_heap);
    }

    OSM_LOG_EXIT(p_mgr->p_log);
    return 0;
}

static int rues_build_graph(void *context)
{
    rues_context_t *rues_context = (rues_context_t *) context;
    osm_ucast_mgr_t *p_mgr = (osm_ucast_mgr_t *) (rues_context->p_mgr);
    boolean_t has_fdr10 = (1 == p_mgr->p_subn->opt.fdr10) ? TRUE : FALSE;
    cl_qmap_t *port_tbl = &p_mgr->p_subn->port_guid_tbl;	/* 1 management port per switch + 1 or 2 ports for each Hca */
    osm_port_t *p_port = NULL;
    cl_qmap_t *sw_tbl = &p_mgr->p_subn->sw_guid_tbl;
    cl_map_item_t *item = NULL;
    osm_switch_t *sw = NULL;
    osm_node_t *remote_node = NULL;
    uint8_t port = 0, remote_port = 0, max_lmc = 1;
    uint32_t i = 0, j = 0, err = 0, undiscov = 0, max_num_undiscov = 0;
    // counts each individual lid
    uint64_t total_num_hca = 0;
    vertex_t *adj_list = NULL;
    osm_physp_t *p_physp = NULL;
    link_t *link = NULL, *head = NULL;
    boolean_t *mapping = NULL;
    uint32_t num_sw = 0, adj_list_size = 0;
    uint8_t lmc = 0;
    uint16_t sm_lid = 0;
    cl_heap_t heap;

    OSM_LOG_ENTER(p_mgr->p_log);
    OSM_LOG(p_mgr->p_log, OSM_LOG_VERBOSE,
            "Building graph for rues routing\n");

    /* if this pointer isn't NULL, this is a reroute step;
       old context will be destroyed (adj_list and srcdest2vl_table)
       */
    if (rues_context->adj_list)
        rues_context_destroy(context);

    /* construct the generic heap opject to use it in dijkstra */
    cl_heap_construct(&heap);

    num_sw = cl_qmap_count(sw_tbl);
    adj_list_size = num_sw + 1;


    /* allocate an adjazenz list (array), 0. element is reserved for the source (Hca) in the routing algo, others are switches */
    adj_list = (vertex_t *) malloc(adj_list_size * sizeof(vertex_t));
    if (!adj_list) {
        OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                "ERR AD02: cannot allocate memory for adj_list\n");
        goto ERROR;
    }
    for (i = 0; i < adj_list_size; i++)
        set_default_vertex(&adj_list[i]);

    rues_context->adj_list = adj_list;
    rues_context->adj_list_size = adj_list_size;

    /* count the total number of Hca / LIDs (for lmc>0) in the fabric;
       even include base/enhanced switch port 0; base SP0 will have lmc=0
       */
    for (item = cl_qmap_head(port_tbl); item != cl_qmap_end(port_tbl);
            item = cl_qmap_next(item)) {
        p_port = (osm_port_t *) item;
        if (osm_node_get_type(p_port->p_node) == IB_NODE_TYPE_CA ||
                osm_node_get_type(p_port->p_node) == IB_NODE_TYPE_SWITCH) {
            lmc = osm_port_get_lmc(p_port);
            max_lmc = max(max_lmc, 1 << lmc);
            total_num_hca += (1 << lmc);
        }
    }
    rues_context->number_of_layers = max_lmc;

    i = 1;			/* fill adj_list -> start with index 1 because the 0. element is reserved */
    for (item = cl_qmap_head(sw_tbl); item != cl_qmap_end(sw_tbl);
            item = cl_qmap_next(item), i++) {
        sw = (osm_switch_t *) item;
        OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
                "Processing switch with GUID 0x%" PRIx64 "\n",
                cl_ntoh64(osm_node_get_node_guid(sw->p_node)));

        adj_list[i].guid =
            cl_ntoh64(osm_node_get_node_guid(sw->p_node));
        adj_list[i].lid =
            cl_ntoh16(osm_node_get_base_lid(sw->p_node, 0));
        adj_list[i].sw = sw;

        link = (link_t *) malloc(sizeof(link_t));
        if (!link) {
            OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                    "ERR AD03: cannot allocate memory for a link\n");
            goto ERROR;
        }

        head = link;
        head->next = NULL;

        /* add SP0 to number of CA connected to a switch */
        lmc = osm_node_get_lmc(sw->p_node, 0);
        adj_list[i].num_hca += (1 << lmc);

        /* iterate over all ports in the switch, start with port 1 (port 0 is a management port) */
        for (port = 1; port < sw->num_ports; port++) {
            /* get the node behind the port */
            remote_node =
                osm_node_get_remote_node(sw->p_node, port,
                        &remote_port);
            /* if there is no remote node on this port or it's the same switch -> try next port */
            if (!remote_node || remote_node->sw == sw)
                continue;
            /* make sure the link is healthy */
            p_physp = osm_node_get_physp_ptr(sw->p_node, port);
            if (!p_physp || !osm_link_is_healthy(p_physp))
                continue;
            /* if there is a Hca connected -> count and cycle */
            if (!remote_node->sw) {
                lmc = osm_node_get_lmc(remote_node, (uint32_t)remote_port);
                adj_list[i].num_hca += (1 << lmc);
                continue;
            }
            /* filter out throttled links to improve performance */
            if (p_mgr->p_subn->opt.avoid_throttled_links &&
                    osm_link_is_throttled(p_physp, has_fdr10)) {
                OSM_LOG(p_mgr->p_log, OSM_LOG_INFO,
                        "Detected and ignoring throttled link:"
                        " 0x%" PRIx64 "/P%" PRIu8
                        " <--> 0x%" PRIx64 "/P%" PRIu8 "\n",
                        cl_ntoh64(osm_node_get_node_guid(sw->p_node)),
                        port,
                        cl_ntoh64(osm_node_get_node_guid(remote_node)),
                        remote_port);
                continue;
            }
            OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
                    "Node 0x%" PRIx64 ", remote node 0x%" PRIx64
                    ", port %" PRIu8 ", remote port %" PRIu8 "\n",
                    cl_ntoh64(osm_node_get_node_guid(sw->p_node)),
                    cl_ntoh64(osm_node_get_node_guid(remote_node)),
                    port, remote_port);

            link->next = (link_t *) malloc(sizeof(link_t));
            if (!link->next) {
                OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                        "ERR AD08: cannot allocate memory for a link\n");
                while (head) {
                    link = head;
                    head = head->next;
                    free(link);
                }
                goto ERROR;
            }
            link = link->next;
            set_default_link(link);

            mapping = (boolean_t *) malloc(rues_context->number_of_layers * sizeof(boolean_t));
            if(!mapping) {
                OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                        "ERR AD03: cannot allocate memory for a layer to present mapping of a link\n");
                while (head) {
                    link = head;
                    head = head->next;
                    free(link);
                }
                goto ERROR;
            }
            for(j = 0; j < rues_context->number_of_layers; j++)
                mapping[j] = TRUE;
            link->layer_mapping = mapping; 

            link->guid =
                cl_ntoh64(osm_node_get_node_guid(remote_node));
            link->from = i;
            link->from_port = port;
            link->to_port = remote_port;
            link->weight = total_num_hca * total_num_hca;	/* initialize with P^2 to force shortest paths */
        }

        adj_list[i].links = head->next;
        free(head);
    }
    /* connect the links with it's second adjacent node in the list */
    for (i = 1; i < adj_list_size; i++) {
        link = adj_list[i].links;
        while (link) {
            for (j = 1; j < adj_list_size; j++) {
                if (link->guid == adj_list[j].guid) {
                    link->to = j;
                    break;
                }
            }
            link = link->next;
        }
    }

    /* do one dry run to determine connectivity issues */
    sm_lid = p_mgr->p_subn->master_sm_base_lid;
    p_port = osm_get_port_by_lid(p_mgr->p_subn, sm_lid);
    err = dijkstra(p_mgr, &heap, adj_list, adj_list_size, p_port, sm_lid, false, 0);

    if (err) {
        goto ERROR;
    } else {
        /* if sm is running on a switch, then dijkstra doesn't
           initialize the used_link for this switch
           */
        if (osm_node_get_type(p_port->p_node) != IB_NODE_TYPE_CA)
            max_num_undiscov = 1;
        for (i = 1; i < adj_list_size; i++) {
            if(!adj_list[i].used_link) {
                undiscov++;
                OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG, 
                        "Switch: 0x%" PRIx64 ", was not discovered by dijkstra\n", 
                        cl_ntoh64(osm_node_get_node_guid(adj_list[i].sw->p_node)));
            }
        }
        if (max_num_undiscov < undiscov) {
            OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                    "ERR AD0C: unsupported network state (detached"
                    " and inaccessible switches found; gracefully"
                    " shutdown this routing engine)\n");
            goto ERROR;
        }
    }
    /* delete the heap which is not needed anymore */
    cl_heap_destroy(&heap);

    /* print the discovered graph */
    if (OSM_LOG_IS_ACTIVE_V2(p_mgr->p_log, OSM_LOG_DEBUG))
        print_graph(p_mgr, adj_list, adj_list_size);

    OSM_LOG_EXIT(p_mgr->p_log);
    return 0;

ERROR:
    if (cl_is_heap_inited(&heap))
        cl_heap_destroy(&heap);
    rues_context_destroy(context);
    return -1;
}

/* update the linear forwarding tables of all switches with the informations
   from the last dijsktra step
*/
static int update_lft(osm_ucast_mgr_t * p_mgr, vertex_t * adj_list,
		      uint32_t adj_list_size, osm_port_t * p_port, uint16_t lid, boolean_t ensure_connected)
{
	uint32_t i = 0;
	uint8_t port = 0;
	uint8_t hops = 0;
	osm_switch_t *p_sw = NULL;
	boolean_t is_ignored_by_port_prof = FALSE;
	osm_physp_t *p = NULL;
	cl_status_t ret;

	OSM_LOG_ENTER(p_mgr->p_log);

	for (i = 1; i < adj_list_size; i++) {
		/* if no route goes thru this switch -> cycle */
		if (!(adj_list[i].used_link))
			continue;

		p_sw = adj_list[i].sw;
		hops = adj_list[i].hops;
		port = adj_list[i].used_link->to_port;
		/* the used_link is the link that was used in dijkstra to reach this node,
		   so the to_port is the local port on this node
		 */

		if (port == OSM_NO_PATH) {	/* if clause shouldn't be possible in this routing, but who cares */
			OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
				"ERR AD06: No path to get to LID %" PRIu16
				" from switch 0x%" PRIx64 "\n", lid,
				cl_ntoh64(osm_node_get_node_guid
					  (p_sw->p_node)));
            if(!ensure_connected)
                continue;
			return 1;
		} else {
			OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
				"Routing LID %" PRIu16 " to port %" PRIu8
				" for switch 0x%" PRIx64 "\n", lid, port,
				cl_ntoh64(osm_node_get_node_guid
					  (p_sw->p_node)));

			p = osm_node_get_physp_ptr(p_sw->p_node, port);
			if (!p) {
				OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
					"ERR AD0A: Physical port %d of Node GUID 0x%"
					PRIx64 "not found\n", port,
					cl_ntoh64(osm_node_get_node_guid(p_sw->p_node)));
				return 1;
			}

			/* we would like to optionally ignore this port in equalization
			   as in the case of the Mellanox Anafa Internal PCI TCA port
			 */
			is_ignored_by_port_prof = p->is_prof_ignored;

			/* We also would ignore this route if the target lid is of
			   a switch and the port_profile_switch_node is not TRUE
			 */
			if (!p_mgr->p_subn->opt.port_profile_switch_nodes)
				is_ignored_by_port_prof |=
				    (osm_node_get_type(p_port->p_node) ==
				     IB_NODE_TYPE_SWITCH);
		}

		/* to support lmc > 0 the functions alloc_ports_priv, free_ports_priv, find_and_add_remote_sys
		   from minhop aren't needed cause osm_switch_recommend_path is implicitly calculated
		   for each LID pair thru dijkstra;
		   for each port the dijkstra algorithm calculates (max_lid_ho - min_lid_ho)-times maybe
		   disjoint routes to spread the bandwidth -> diffent routes for one port and lmc>0
		 */

		/* set port in LFT */
		p_sw->new_lft[lid] = port;
		if (!is_ignored_by_port_prof) {
			/* update the number of path routing thru this port */
			osm_switch_count_path(p_sw, port);
		}
		/* set the hop count from this switch to the lid */
		ret = osm_switch_set_hops(p_sw, lid, port, hops);
		if (ret != CL_SUCCESS)
			OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
				"ERR AD05: cannot set hops for LID %" PRIu16
				" at switch 0x%" PRIx64 "\n", lid,
				cl_ntoh64(osm_node_get_node_guid
					  (p_sw->p_node)));
	}

	OSM_LOG_EXIT(p_mgr->p_log);
	return 0;
}

/* increment the edge weights of the df-/sssp graph which represent the number
   of paths on this link
*/
static void update_weights(osm_ucast_mgr_t * p_mgr, vertex_t * adj_list,
			   uint32_t adj_list_size, uint16_t lid)
{
	uint32_t i = 0, j = 0;
	uint32_t additional_weight = 0;

	OSM_LOG_ENTER(p_mgr->p_log);

	for (i = 1; i < adj_list_size; i++) {
		/* if no route goes thru this switch or the route was present before the dijkstra step we just performed -> cycle */
		if (!(adj_list[i].used_link))
			continue;
		additional_weight = adj_list[i].num_hca;

		j = i;
		while (adj_list[j].used_link) {
			/* update the link from pre to this node */
			adj_list[j].used_link->weight += additional_weight;

			j = adj_list[j].used_link->from;
		}
	}

	OSM_LOG_EXIT(p_mgr->p_log);
}

static int fill_lft_entries(rues_context_t *rues_context, osm_ucast_mgr_t *p_mgr)
{
	vertex_t *adj_list = (vertex_t *) rues_context->adj_list;
	uint32_t adj_list_size = rues_context->adj_list_size;
	cl_heap_t heap;

	cl_qlist_t *qlist = NULL;
	cl_list_item_t *qlist_item = NULL;

	cl_qmap_t *sw_tbl = &p_mgr->p_subn->sw_guid_tbl;
	cl_map_item_t *item = NULL;
	osm_switch_t *sw = NULL;
	osm_port_t *port = NULL;
	uint32_t i = 0, err = 0;
	uint16_t lid = 0, min_lid_ho = 0, max_lid_ho = 0;

	OSM_LOG_ENTER(p_mgr->p_log);
	OSM_LOG(p_mgr->p_log, OSM_LOG_VERBOSE,
		"Calculating shortest path from all Hca/switches to all\n");

    cl_heap_construct(&heap);

	/* do the routing for the each Hca in the subnet and each switch
	   in the subnet (to add the routes to base/enhanced SP0)
	 */
	qlist = &p_mgr->port_order_list;
	for (qlist_item = cl_qlist_head(qlist);
	     qlist_item != cl_qlist_end(qlist);
	     qlist_item = cl_qlist_next(qlist_item)) {
		port = (osm_port_t *)cl_item_obj(qlist_item, port, list_item);

		/* calculate shortest path with dijkstra from node to all switches/Hca */
		if (osm_node_get_type(port->p_node) == IB_NODE_TYPE_CA) {
			OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
				"Processing Hca with GUID 0x%" PRIx64 "\n",
				cl_ntoh64(osm_node_get_node_guid
					  (port->p_node)));
		} else if (osm_node_get_type(port->p_node) == IB_NODE_TYPE_SWITCH) {
			OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
				"Processing switch with GUID 0x%" PRIx64 "\n",
				cl_ntoh64(osm_node_get_node_guid
					  (port->p_node)));
		} else {
			/* we don't handle routers, in case they show up */
			continue;
		}

		osm_port_get_lid_range_ho(port, &min_lid_ho,
					  &max_lid_ho);
		for (lid = min_lid_ho; lid <= max_lid_ho; lid++) {
			/* do dijkstra from this Hca/LID/SP0 to each switch */
			err =
			    dijkstra(p_mgr, &heap, adj_list, adj_list_size,
				     port, lid, true, lid-min_lid_ho);
			if (err)
				goto ERROR;
			if (OSM_LOG_IS_ACTIVE_V2(p_mgr->p_log, OSM_LOG_DEBUG))
				print_routes(p_mgr, adj_list, adj_list_size,
					     port);

			/* make an update for the linear forwarding tables of the switches */
			err = update_lft(p_mgr, adj_list, adj_list_size, port, lid, rues_context->ensure_connected);
			if (err)
				goto ERROR;

			update_weights(p_mgr, adj_list, adj_list_size, lid);

			if (OSM_LOG_IS_ACTIVE_V2(p_mgr->p_log, OSM_LOG_DEBUG))
				print_graph(p_mgr, adj_list,
						   adj_list_size);
		}
	}

	/* delete the heap which is not needed anymore */
	cl_heap_destroy(&heap);

	/* print the new_lft for each switch after routing is done */
	if (OSM_LOG_IS_ACTIVE_V2(p_mgr->p_log, OSM_LOG_DEBUG)) {
		for (item = cl_qmap_head(sw_tbl); item != cl_qmap_end(sw_tbl);
		     item = cl_qmap_next(item)) {
			sw = (osm_switch_t *) item;
			OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
				"Summary of the (new) LFT for switch 0x%" PRIx64
				" (%s):\n",
				cl_ntoh64(osm_node_get_node_guid(sw->p_node)),
				sw->p_node->print_desc);
			for (i = 0; i < sw->max_lid_ho + 1; i++)
				if (sw->new_lft[i] != OSM_NO_PATH) {
					OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
						"   for LID=%" PRIu32
						" use port=%" PRIu8 "\n", i,
						sw->new_lft[i]);
				}
		}
	}

	OSM_LOG_EXIT(p_mgr->p_log);
    return 0;

ERROR:
	if (cl_is_heap_inited(&heap))
		cl_heap_destroy(&heap);
    return -1;
}

static inline uint8_t random_number(uint8_t min, uint8_t range) 
{
    return rand()%range + min;
}

static int rues_generate_layer(rues_context_t *rues_context, osm_ucast_mgr_t *p_mgr, uint8_t layer_number)
{
    uint32_t adj_list_size = rues_context->adj_list_size;
    vertex_t *adj_list = rues_context->adj_list;
    uint32_t i = 0, undiscov = 0, max_num_undiscov = 0, err = 0;
    link_t *link = NULL;
    link_t *rev_link = NULL;
    boolean_t connected = FALSE;

    osm_port_t *p_port = NULL;
    uint16_t sm_lid = 0;
    boolean_t state = TRUE;
    cl_heap_t heap;
    cl_heap_construct(&heap);

    do {
        for(i = 1; i < adj_list_size; i++) {
            link = adj_list[i].links;
            while(link != NULL) {
                if(!link->layer_mapping && (layer_number || !rues_context->first_layer_complete))
                    goto ERROR;
                if(!layer_number && rues_context->first_layer_complete)
                    break;
                // So links are only checked once
                if(link->from > link->to) {
                    link = link->next;
                    continue;
                }
                // find reverse link
                rev_link = adj_list[link->to].links;
                while(rev_link != NULL) {
                    if(rev_link->to == link->from && rev_link->from_port == link->to_port) {
                        break;
                    }
                    rev_link = rev_link->next;
                }
                if(!rev_link) {
                    OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
                            "Couldn't find reverse link for src_sw_lid: %" PRIu16 "  dst_sw_lid = %" PRIu16 "\n", 
                            adj_list[link->from].lid, adj_list[link->to].lid);
                    goto ERROR;
                }
                state = random_number(0, 100) < rues_context->p;
                link->layer_mapping[layer_number] = state;
                rev_link->layer_mapping[layer_number] = state;
                link = link->next;
            }
        }

        sm_lid = p_mgr->p_subn->master_sm_base_lid;
        p_port = osm_get_port_by_lid(p_mgr->p_subn, sm_lid);
        err = dijkstra(p_mgr, &heap, adj_list, adj_list_size, p_port, sm_lid, true, layer_number);

        if (err) {
            goto ERROR;
        } else {
            /* if sm is running on a switch, then dijkstra doesn't
               initialize the used_link for this switch
               */
            if (osm_node_get_type(p_port->p_node) != IB_NODE_TYPE_CA)
                max_num_undiscov = 1;
            for (i = 1; i < adj_list_size; i++) {
                if(!adj_list[i].used_link) {
                    undiscov++;
                    OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG, 
                            "Switch: 0x%" PRIx64 ", was not discovered by dijkstra\n", 
                            cl_ntoh64(osm_node_get_node_guid(adj_list[i].sw->p_node)));
                }
            }
            connected = max_num_undiscov >= undiscov;
            if(!connected) {
                OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
                    "RUES failed to establish a connected layer, p is %" PRIu8 " and the num undiscovered is %" PRIu32 " go again\n",
                    rues_context->p, undiscov);
            }
	    undiscov = 0;
        }
    } while (rues_context->ensure_connected && !connected);

    cl_heap_destroy(&heap);

    return 0;
ERROR:
    if (cl_is_heap_inited(&heap))
        cl_heap_destroy(&heap);
    return -1;
}

static int rues_perform_routing(void *context)
{
    rues_context_t *rues_context = (rues_context_t *) context;
    osm_ucast_mgr_t *p_mgr = (osm_ucast_mgr_t *) rues_context->p_mgr;
	vertex_t *adj_list = (vertex_t *) rues_context->adj_list;
    uint32_t adj_list_size = rues_context->adj_list_size, sw_list_size = 0;
    uint32_t i = 0, j = 0;
    uint8_t layer_number = 0, lmc = 0;
    uint16_t min_lid_ho = 0;
	uint64_t guid = 0;

    link_t *link = NULL;

    cl_qmap_t *sw_tbl = &p_mgr->p_subn->sw_guid_tbl;
	vertex_t **sw_list = NULL;
    cl_map_item_t *item = NULL;
    osm_switch_t *sw = NULL;

	cl_qmap_t cn_tbl, io_tbl, *p_mixed_tbl = NULL;
	boolean_t cn_nodes_provided = FALSE, io_nodes_provided = FALSE;


	cl_qmap_init(&cn_tbl);
	cl_qmap_init(&io_tbl);
	p_mixed_tbl = &cn_tbl;

    cl_qlist_init(&p_mgr->port_order_list);

    /* reset the new_lft for each switch */
    for (item = cl_qmap_head(sw_tbl); item != cl_qmap_end(sw_tbl);
            item = cl_qmap_next(item)) {
        sw = (osm_switch_t *) item;
        /* initialize LIDs in buffer to invalid port number */
        memset(sw->new_lft, OSM_NO_PATH, sw->max_lid_ho + 1);
        /* initialize LFT and hop count for bsp0/esp0 of the switch */
        min_lid_ho = cl_ntoh16(osm_node_get_base_lid(sw->p_node, 0));
        lmc = osm_node_get_lmc(sw->p_node, 0);
        for (i = min_lid_ho; i < min_lid_ho + (1 << lmc); i++) {
            /* for each switch the port to the 'self'lid is the management port 0 */
            sw->new_lft[i] = 0;
            /* the hop count to the 'self'lid is 0 for each switch */
            osm_switch_set_hops(sw, i, 0, 0);
        }
    }

	/* we need an intermediate array of pointers to switches in adj_list;
	   this array will be sorted in respect to num_hca (descending)
	 */
	sw_list_size = adj_list_size - 1;
	sw_list = (vertex_t **)malloc(sw_list_size * sizeof(vertex_t *));
	if (!sw_list) {
		OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
			"ERR AD29: cannot allocate memory for sw_list in dfsssp_do_dijkstra_routing\n");
		goto ERROR;
	}
	memset(sw_list, 0, sw_list_size * sizeof(vertex_t *));

	/* fill the array with references to the 'real' sw in adj_list */
	for (i = 0; i < sw_list_size; i++)
		sw_list[i] = &(adj_list[i + 1]);

	/* sort the sw_list in descending order */
	sw_list_sort_by_num_hca(sw_list, sw_list_size);

	/* parse compute node guid file, if provided by the user */
	if (p_mgr->p_subn->opt.cn_guid_file) {
		OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
			"Parsing compute nodes from file %s\n",
			p_mgr->p_subn->opt.cn_guid_file);

		if (parse_node_map(p_mgr->p_subn->opt.cn_guid_file,
				   add_guid_to_map, &cn_tbl)) {
			OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
				"ERR AD33: Problem parsing compute node guid file\n");
			goto ERROR;
		}

		if (cl_is_qmap_empty(&cn_tbl))
			OSM_LOG(p_mgr->p_log, OSM_LOG_INFO,
				"WRN AD34: compute node guids file contains no valid guids\n");
		else
			cn_nodes_provided = TRUE;
	}

	/* parse I/O guid file, if provided by the user */
	if (p_mgr->p_subn->opt.io_guid_file) {
		OSM_LOG(p_mgr->p_log, OSM_LOG_DEBUG,
			"Parsing I/O nodes from file %s\n",
			p_mgr->p_subn->opt.io_guid_file);

		if (parse_node_map(p_mgr->p_subn->opt.io_guid_file,
				   add_guid_to_map, &io_tbl)) {
			OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
				"ERR AD35: Problem parsing I/O guid file\n");
			goto ERROR;
		}

		if (cl_is_qmap_empty(&io_tbl))
			OSM_LOG(p_mgr->p_log, OSM_LOG_INFO,
				"WRN AD36: I/O node guids file contains no valid guids\n");
		else
			io_nodes_provided = TRUE;
	}

	/* if we mix Hca/Tca/SP0 during the dijkstra routing, we might end up
	   in rare cases with a bad balancing for Hca<->Hca connections, i.e.
	   some inter-switch links get oversubscribed with paths;
	   therefore: add Hca ports first to ensure good Hca<->Hca balancing
	 */
	if (cn_nodes_provided) {
		for (i = 0; i < adj_list_size - 1; i++) {
			if (sw_list[i] && sw_list[i]->sw) {
				sw = (osm_switch_t *)(sw_list[i]->sw);
				add_sw_endports_to_order_list(sw, p_mgr,
							      &cn_tbl, TRUE);
			} else {
				OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
					"ERR AD30: corrupted sw_list array in dfsssp_do_dijkstra_routing\n");
				goto ERROR;
			}
		}
	}
	/* then: add Tca ports to ensure good Hca->Tca balancing and separate
	   paths towards I/O nodes on the same switch (if possible)
	 */
	if (io_nodes_provided) {
		for (i = 0; i < adj_list_size - 1; i++) {
			if (sw_list[i] && sw_list[i]->sw) {
				sw = (osm_switch_t *)(sw_list[i]->sw);
				add_sw_endports_to_order_list(sw, p_mgr,
							      &io_tbl, TRUE);
			} else {
				OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
					"ERR AD32: corrupted sw_list array in dfsssp_do_dijkstra_routing\n");
				goto ERROR;
			}
		}
	}
	/* then: add anything else, such as administration nodes, ... */
	if (cn_nodes_provided && io_nodes_provided) {
		cl_qmap_merge(&cn_tbl, &io_tbl);
	} else if (io_nodes_provided) {
		p_mixed_tbl = &io_tbl;
	}
	for (i = 0; i < adj_list_size - 1; i++) {
		if (sw_list[i] && sw_list[i]->sw) {
			sw = (osm_switch_t *)(sw_list[i]->sw);
			add_sw_endports_to_order_list(sw, p_mgr, p_mixed_tbl,
						      FALSE);
		} else {
			OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
				"ERR AD39: corrupted sw_list array in dfsssp_do_dijkstra_routing\n");
			goto ERROR;
		}
	}

	/* last: add SP0 afterwards which have lower priority for balancing */
	for (i = 0; i < sw_list_size; i++) {
		if (sw_list[i] && sw_list[i]->sw) {
			sw = (osm_switch_t *)(sw_list[i]->sw);
			guid = cl_ntoh64(osm_node_get_node_guid(sw->p_node));
			add_guid_to_order_list(guid, p_mgr);
		} else {
			OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
				"ERR AD31: corrupted sw_list array in dfsssp_do_dijkstra_routing\n");
			goto ERROR;
		}
	}

	/* the intermediate array lived long enough */
	free(sw_list);
	sw_list = NULL;
	/* same is true for the compute node and I/O guid map */
	destroy_guid_map(&cn_tbl);
	cn_nodes_provided = FALSE;
	destroy_guid_map(&io_tbl);
	io_nodes_provided = FALSE;

    /* reset selected links */
    for(i = 1; i < adj_list_size; i++) {
        link = adj_list[i].links;
        while(link) {
            if(link->layer_mapping) {
                for(j = 0; j < rues_context->number_of_layers; j++) {
                    link->layer_mapping[j] = TRUE;
                }
            }
            link = link->next;
        }
    }

    /* generate layers */

    for(layer_number = 1; layer_number < rues_context->number_of_layers; layer_number++) {
        rues_generate_layer(rues_context, p_mgr, layer_number);
        print_layer(rues_context, p_mgr, layer_number);
    }

    // fill entries using dijkstra
    fill_lft_entries(rues_context, p_mgr);

    // create temporary dfsssp_context to remove deadlocks
    dfsssp_context_t dfsssp_ctx = { .routing_type = OSM_ROUTING_ENGINE_TYPE_DFSSSP, .p_mgr = p_mgr,
        .adj_list = rues_context->adj_list, .adj_list_size = rues_context->adj_list_size, .srcdest2vl_table = NULL, .vl_split_count = NULL, .only_best_effort = p_mgr->p_subn->opt.dfsssp_best_effort };
    
    if(rues_context->apply_dfsssp) {
        if(dfsssp_remove_deadlocks(&dfsssp_ctx))
            goto ERROR;
        rues_context->srcdest2vl_table = dfsssp_ctx.srcdest2vl_table;
        rues_context->vl_split_count = dfsssp_ctx.vl_split_count;
    } else {
       OSM_LOG(p_mgr->p_log, OSM_LOG_INFO,
            "No deadlock removal specified -> skipping deadlock removal through dfsssp_remove_deadlocks(...)\n");
    }
    
	/* list not needed after the dijkstra steps and deadlock removal */
	cl_qlist_remove_all(&p_mgr->port_order_list);
    return 0; 

ERROR:
	if (cn_nodes_provided)
		destroy_guid_map(&cn_tbl);
	if (io_nodes_provided)
		destroy_guid_map(&io_tbl);
	if (sw_list)
		free(sw_list);
	if (!cl_is_qlist_empty(&p_mgr->port_order_list))
		cl_qlist_remove_all(&p_mgr->port_order_list);
    return -1;
}

static uint8_t get_rues_sl(void *context, uint8_t hint_for_default_sl, const ib_net16_t slid, const ib_net16_t dlid)
{
	rues_context_t *rues_context = (rues_context_t *) context;
	osm_port_t *src_port, *dest_port;
	vltable_t *srcdest2vl_table = NULL;
	uint8_t *vl_split_count = NULL;
	osm_ucast_mgr_t *p_mgr = NULL;
	int32_t res = 0;

    if (rues_context) {
		p_mgr = (osm_ucast_mgr_t *) rues_context->p_mgr;
		srcdest2vl_table = (vltable_t *) (rues_context->srcdest2vl_table);
		vl_split_count = (uint8_t *) (rues_context->vl_split_count);
	}
	else
		return hint_for_default_sl;

	src_port = osm_get_port_by_lid(p_mgr->p_subn, slid);
	if (!src_port)
		return hint_for_default_sl;

	dest_port = osm_get_port_by_lid(p_mgr->p_subn, dlid);
	if (!dest_port)
		return hint_for_default_sl;

	if (!srcdest2vl_table)
		return hint_for_default_sl;

	res = vltable_get_vl(srcdest2vl_table, slid, dlid);

	/* we will randomly distribute the traffic over multiple VLs if
	   necessary for good balancing; therefore vl_split_count provides
	   the number of VLs to use for certain traffic
	 */
	if (res > -1) {
		if (vl_split_count[res] > 1)
			return (uint8_t) (res + rand()%(vl_split_count[res]));
		else
			return (uint8_t) res;
	} else
		return hint_for_default_sl;
    return hint_for_default_sl;
}

static ib_api_status_t rues_do_mcast_routing(void *context, osm_mgrp_box_t *mbox)
{
    rues_context_t *rues_context = (rues_context_t *) context;

    // create temporary dfsssp_context to call dfsssp's mcast routing
    dfsssp_context_t dfsssp_ctx = { .routing_type = OSM_ROUTING_ENGINE_TYPE_DFSSSP, .p_mgr = rues_context->p_mgr,
        .adj_list = rues_context->adj_list, .adj_list_size = rues_context->adj_list_size, 
        .srcdest2vl_table = rues_context->srcdest2vl_table, .vl_split_count = rues_context->vl_split_count, .only_best_effort = rues_context->p_mgr->p_subn->opt.dfsssp_best_effort };

    return dfsssp_do_mcast_routing(&dfsssp_ctx, mbox);
}

int osm_ucast_rues_setup(struct osm_routing_engine *r, osm_opensm_t *p_osm)
{
    rues_context_t *rues_context = rues_context_create(p_osm, OSM_ROUTING_ENGINE_TYPE_RUES);
    if(!rues_context) {
        return 1; /* alloc failed -> skip this routing */
    }

    /* reset funciton pointer to rues routines */
    r->context = (void *)rues_context;
    r->build_lid_matrices = rues_build_graph;
    r->ucast_build_fwd_tables = rues_perform_routing;
    r->mcast_build_stree = rues_do_mcast_routing;

    r->path_sl = get_rues_sl; 
    r->destroy = delete;

    /* we initialize with the current time to achieve a 'good' randomized
       assignment in get_dfsssp_sl(...)
       */
    srand(time(NULL));

    return 0;
}
