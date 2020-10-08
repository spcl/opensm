#if HAVE_CONGIF_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <complib/cl_heap.h>

#include <opensm/osm_file_ids.h>
#define FILE_ID OSM_FILE_UCAST_LNMP_C
#include <opensm/osm_ucast_mgr.h>
#include <opensm/osm_opensm.h>
#include <opensm/osm_node.h>
#include <opensm/osm_multicast.h>
#include <opensm/osm_mcast_mgr.h>

#include <opensm/osm_ucast_dfsssp.h>

#include <stdbool.h>

#define max(x,y) ((x) >= (y)) ? (x) : (y)
#define min(x,y) ((x) <= (y)) ? (x) : (y)

typedef struct lnmp_context {
    osm_routing_engine_type_t routing_type;
    osm_ucast_mgr_t *p_mgr;
    vertex_t *adj_list;
    uint32_t adj_list_size;
    vltable_t *srcdest2vl_table;
    uint8_t *vl_split_count;
    uint8_t number_of_layers;
    uint64_t maximum_number_of_paths;
    uint8_t min_length;
    uint8_t max_length;
    vertex_t **layers;
} lnmp_context_t;

typedef struct node {
    struct node *l, *r;
    uint64_t value;
    uint8_t height; // maximum height is <1.45 * log (n) <= 1.45 * 64 < 2*64 = 2**7
} node_t;

typedef struct sd_pair {
    uint16_t first_base_lid;
    uint16_t second_base_lid;
    uint16_t priority_level;
} sd_pair_t;

/*
 * ---------------------------------------------------
 * AVL Tree Implementation
 * ---------------------------------------------------
 */
static node_t *new_node(uint64_t val) 
{
    node_t *new = (node_t *) malloc(sizeof(node_t));
    if (!new)
        return NULL;
    new->l = NULL;
    new->r = NULL;
    new->value = val;
    new->height = 1;
    return new;
}

static void destroy_node(node_t *node) 
{
    if(node->l)
        destroy_node(node->l);
    if(node->r)
        destroy_node(node->r);
    free(node);
}

static uint8_t get_height(node_t *root)
{
    return (root) ? root->height : 0;
}

static void reset_height(node_t *root)
{
    if(root)
        root->height = 1 + max(get_height(root->l), get_height(root->r));
}

static void right_rotate(node_t **root)
{
    node_t *temp1 = (*root)->l;
    node_t *temp2 = temp1->r;
    temp1->r = (*root);
    (*root)->l = temp2;
    reset_height(temp1);
    reset_height(*root);
    *root = temp1;
}

static void left_rotate(node_t **root)
{
    node_t *temp1 = (*root)->r;
    node_t *temp2 = temp1->l;
    temp1->l = (*root);
    (*root)->r = temp2;
    reset_height(temp1);
    reset_height(*root);
    *root = temp1;
}

static int get_balance(node_t *root)
{
    return (root) ? get_height(root->l) - get_height(root->r) : 0;
}

static void insert(node_t **root, node_t *node) 
{
    if(!*root)
        *root = node;
    else
        insert(node->value <= (*root)->value ? &(*root)->l : &(*root)->r, node);

    int balance = get_balance(*root);

    if(balance > 1 && node->value < (*root)->l->value) {
        right_rotate(root); 
    } else if (balance < -1 && node->value > (*root)->r->value) {
        left_rotate(root);
    } else if (balance > 1 && node->value > (*root)->l->value) {
        left_rotate(&(*root)->l);
        right_rotate(root);
    } else if (balance < -1 && node->value < (*root)->r->value) {
        right_rotate(&(*root)->r);
        left_rotate(root);
    }
}

static node_t *minValueNode(node_t *root) 
{
    while(root->l)
        root = root->l;
    return root;
}

static void delete_node(node_t **root, int64_t val)
{
    if(!*root)
        return;
    if(val > (*root)->value)
        delete_node(&(*root)->r, val);
    else if (val < (*root)->value)
        delete_node(&(*root)->l, val);
    else {
        if (!(*root)->l || !(*root)->r) {
            node_t *temp = (*root)->l ? (*root)->l : (*root)->r;
            free(*root);
            if(!temp) {
                *root = NULL;
            } else {
                *root = temp;
            }
        } else {
            node_t *temp = minValueNode((*root)->r);
            (*root)->value = temp->value;
            delete_node(&(*root)->r, temp->value);
        }
    }
    if(!*root)
        return;

    reset_height(*root);
    int balance = get_balance(*root);

    if(balance > 1 && get_balance((*root)->l) >= 0) {
        right_rotate(root); 
    } else if(balance > 1 && get_balance((*root)->l) < 0) {
        left_rotate(&(*root)->l);
        right_rotate(root);
    } else if (balance < -1 && get_balance((*root)->r) <= 0) {
        left_rotate(root);
    } else if (balance < -1 && get_balance((*root)->r) > 0) {
        right_rotate(&(*root)->r);
        left_rotate(root);
    }
}

static void find(node_t *root, node_t **node, int64_t val) 
{
    if(!root)
        node = NULL;
    else if(root->value == val)
        *node = root;
    else
        find(val <= root->value ? root->l : root->r, node, val);
}

static boolean_t contains(node_t *root, int64_t val) 
{
    if(!root)
        return FALSE;
    else if(root->value == val)
        return TRUE;
    else
        return contains(val <= root->value ? root->l : root->r, val);
}

/*
 * if direction == FALSE ? decreasing : increasing
 */
static void add_from_offset(node_t *root, uint32_t *index, uint64_t *array, boolean_t direction)
{
    if(root) {
        array[*index] = root->value;
        (direction) ? (*index)++: (*index)--;
        add_from_offset(root->l, index, array, direction);
        add_from_offset(root->r, index, array, direction);
    }
}

/*
 * -------------------------------------------------
 */

static lnmp_context_t *lnmp_context_create(osm_opensm_t *p_osm, osm_routing_engine_type_t routing_type)
{
    lnmp_context_t *lnmp_context = NULL;

    /* allocate memory */
    lnmp_context = (lnmp_context_t *) malloc(sizeof(lnmp_context_t));
    if(lnmp_context) {
        lnmp_context->routing_type = routing_type;
        lnmp_context->p_mgr = (osm_ucast_mgr_t *) & (p_osm->sm.ucast_mgr);
        lnmp_context->adj_list = NULL;
        lnmp_context->adj_list_size = 0;
        lnmp_context->srcdest2vl_table = NULL;
        lnmp_context->vl_split_count = NULL;

        cl_map_item_t *item = NULL;
        osm_port_t *p_port = NULL;
        uint8_t lmc = 1;
        cl_qmap_t *port_tbl = &lnmp_context->p_mgr->p_subn->port_guid_tbl;	/* 1 management port per switch + 1 or 2 ports for each Hca */
        for (item = cl_qmap_head(port_tbl); item != cl_qmap_end(port_tbl);
                item = cl_qmap_next(item)) {
            p_port = (osm_port_t *) item;
            if (osm_node_get_type(p_port->p_node) == IB_NODE_TYPE_CA) {
                lmc = max(osm_port_get_lmc(p_port), 1 << lmc);
            }
        }

        lnmp_context->number_of_layers = lmc;
        lnmp_context->maximum_number_of_paths = 10000;
        lnmp_context->min_length = 3;
        lnmp_context->max_length = 5;
        lnmp_context->layers = NULL;
    } else {
        OSM_LOG(p_osm->sm.ucast_mgr.p_log, OSM_LOG_ERROR,
                "ERR AD04: cannot allocate memory for lnmp_context in lnmp_context_create\n");
        return NULL;
    }

    return lnmp_context;
}

static void free_adj_list(vertex_t **adj_list, uint32_t adj_list_size) {

    if(*adj_list) {
        uint32_t i = 0;
        link_t *link = NULL, *tmp = NULL;

        /* free adj_list */
        for (i = 0; i < adj_list_size; i++) {
            link = (*adj_list)[i].links;
            while (link) {
                tmp = link;
                link = link->next;
                free(tmp);
            }
        }
        free(*adj_list);
        *adj_list = NULL;
    }
}
static void lnmp_context_destroy(void *context)
{
    lnmp_context_t *lnmp_context = (lnmp_context_t *) context;
    vertex_t *adj_list = (vertex_t *) (lnmp_context->adj_list);
    uint32_t j = 0;
    free_adj_list(&adj_list, lnmp_context->adj_list_size);
    lnmp_context->adj_list = NULL;
    for(j = 0; j < lnmp_context->number_of_layers; j++) {
        adj_list = (vertex_t *) (lnmp_context->layers[j]);     
        free_adj_list(&adj_list, lnmp_context->adj_list_size);
    }
    free((vertex_t **) (lnmp_context->layers));
    lnmp_context->layers = NULL;
    lnmp_context->adj_list_size = 0;

    // TODO check this
    /* free srcdest2vl table and the split count information table
       (can be done, because dfsssp_context_destroy is called after
       osm_get_dfsssp_sl)
       */
    vltable_dealloc(&(lnmp_context->srcdest2vl_table));
    lnmp_context->srcdest2vl_table = NULL;

    if (lnmp_context->vl_split_count) {
        free(lnmp_context->vl_split_count);
        lnmp_context->vl_split_count = NULL;
    }
}

static void delete(void *context)
{
    if(!context)
        return;
    lnmp_context_destroy(context);

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
        osm_port_t * port, uint16_t lid)
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
    if (!cl_is_heap_inited(p_heap))
        ret = cl_heap_init(p_heap, adj_list_size, 4,
                &apply_index_update, NULL);
    else
        ret = cl_heap_resize(p_heap, adj_list_size);
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
        // if the link behind the port is a HCA, we start by copying basic information into position 0 of adjl
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

static int lnmp_build_graph(void *context)
{
    lnmp_context_t *lnmp_context = (lnmp_context_t *) context;
    osm_ucast_mgr_t *p_mgr = (osm_ucast_mgr_t *) (lnmp_context->p_mgr);
    boolean_t has_fdr10 = (1 == p_mgr->p_subn->opt.fdr10) ? TRUE : FALSE;
    cl_qmap_t *port_tbl = &p_mgr->p_subn->port_guid_tbl;	/* 1 management port per switch + 1 or 2 ports for each Hca */
    osm_port_t *p_port = NULL;
    cl_qmap_t *sw_tbl = &p_mgr->p_subn->sw_guid_tbl;
    cl_map_item_t *item = NULL;
    osm_switch_t *sw = NULL;
    osm_node_t *remote_node = NULL;
    uint8_t port = 0, remote_port = 0;
    uint32_t i = 0, j = 0, err = 0, undiscov = 0, max_num_undiscov = 0;
    // counts each individual lid
    uint64_t total_num_hca = 0;
    vertex_t *adj_list = NULL;
    vertex_t **layers = NULL;
    osm_physp_t *p_physp = NULL;
    link_t *link = NULL, *head = NULL;
    uint32_t num_sw = 0, adj_list_size = 0;
    uint8_t lmc = 0;
    uint16_t sm_lid = 0;
    cl_heap_t heap;
    uint8_t layer_number = 0;
    vertex_t *layer = NULL;

    OSM_LOG_ENTER(p_mgr->p_log);
    OSM_LOG(p_mgr->p_log, OSM_LOG_VERBOSE,
            "Building graph for lnmp routing\n");

    /* if this pointer isn't NULL, this is a reroute step;
       old context will be destroyed (adj_list and srcdest2vl_table)
       */
    if (lnmp_context->adj_list)
        lnmp_context_destroy(context);

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

    lnmp_context->adj_list = adj_list;
    lnmp_context->adj_list_size = adj_list_size;

    /* allocate the layers array */
    layers = (vertex_t **) malloc(lnmp_context->number_of_layers * sizeof(vertex_t *));
    if(!layers) {
        OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                "ERR AD02: cannot allocate memory for layer pointers\n");
        goto ERROR;
    }
    /* allocate the different layers */
    for(layer_number = 0; layer_number < lnmp_context->number_of_layers; layer_number++) {
        layer = (vertex_t *) malloc(adj_list_size * sizeof(vertex_t));
        if (!layer) {
            OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                    "ERR AD02: cannot allocate memory for one of the layers\n");
            goto ERROR;
        }
        for(i = 0; i < adj_list_size; i++)
            set_default_vertex(&layer[i]); 
        layers[layer_number] = layer;
    }
    lnmp_context->layers = layers;


    /* count the total number of Hca / LIDs (for lmc>0) in the fabric;
       even include base/enhanced switch port 0; base SP0 will have lmc=0
       */
    for (item = cl_qmap_head(port_tbl); item != cl_qmap_end(port_tbl);
            item = cl_qmap_next(item)) {
        p_port = (osm_port_t *) item;
        if (osm_node_get_type(p_port->p_node) == IB_NODE_TYPE_CA ||
                osm_node_get_type(p_port->p_node) == IB_NODE_TYPE_SWITCH) {
            lmc = osm_port_get_lmc(p_port);
            total_num_hca += (1 << lmc);
        }
    }

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
    //
    err = dijkstra(p_mgr, &heap, adj_list, adj_list_size, p_port, sm_lid);

    if (err) {
        goto ERROR;
    } else {
        // TODO check why this is the case
        /* if sm is running on a switch, then dijkstra doesn't
           initialize the used_link for this switch
           */
        if (osm_node_get_type(p_port->p_node) != IB_NODE_TYPE_CA)
            max_num_undiscov = 1;
        for (i = 1; i < adj_list_size; i++)
            undiscov += (adj_list[i].used_link) ? 0 : 1;
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
        dfsssp_print_graph(p_mgr, adj_list, adj_list_size);

    OSM_LOG_EXIT(p_mgr->p_log);
    return 0;

ERROR:
    if (cl_is_heap_inited(&heap))
        cl_heap_destroy(&heap);
    lnmp_context_destroy(context);
    return -1;
}

/*
 * Used to remove all markings.
 * Mostly used when finishing the creation of a layer or to ensure a clean queue at the beginning.
 */
static void remove_markings(cl_map_t **sdp_priority_queue, uint8_t number_of_levels) {
    // TODO could speed this up at the expense of storage
    uint8_t current_level = 0;
    cl_map_iterator_t itr;
    cl_map_iterator_t end;
    bool *used = NULL;
    for (current_level = 0; current_level < number_of_levels; current_level++) {
        itr = cl_map_head(sdp_priority_queue[current_level]);
        end = cl_map_end(sdp_priority_queue[current_level]);
        while(itr != end) {
            used = (bool *) cl_map_obj(itr); 
            *used = false;
            itr = cl_map_next(itr);
        }
    }
}

/*
 * Used to mark a given source destination pair as being used in the current layer
 */
static int mark_sd(cl_map_t **sdp_priority_queue, uint8_t number_of_levels, sd_pair_t *sd_pair, bool increase_level)
{
    uint8_t current_level = 0;
    uint64_t key = ((uint64_t) sd_pair->first_base_lid << 32) + (uint64_t) sd_pair->second_base_lid;
    sd_pair_t *pair = NULL;
    void *successful_insert = NULL;
    bool used = true;

    for (current_level = 0; current_level < number_of_levels; current_level++) {
        pair = (sd_pair_t *) cl_map_get(sdp_priority_queue[current_level], key);
        if (pair) {
            cl_map_remove(sdp_priority_queue[current_level], key);
            if (increase_level && current_level < number_of_levels -1) {
                //The second part of the if-statement should by design always evaluate to true;
                successful_insert = cl_map_insert(sdp_priority_queue[current_level + 1], key, (void *) (&used));
            } else {
                successful_insert = cl_map_insert(sdp_priority_queue[current_level], key, (void *) (&used));
            }
            // unsuccessful insert
            if (!successful_insert)
                goto ERROR;
            break;
        }
    }
    if (!successful_insert) {
        // sd_pair was not yet inserted
        if (increase_level) {
            // Should always enter this if branch if the object was not yet in any of the maps
            successful_insert = cl_map_insert(sdp_priority_queue[1], key, (void *) (&used));
        } else {
            successful_insert = cl_map_insert(sdp_priority_queue[0], key, (void *) (&used));
        }
        // unsuccessful insert
        if (!successful_insert)
            goto ERROR;
    }
    return 0;

ERROR:
    // Was not able to perform the insertion, it could be the case that key was removed. TODO: handle
    return -1;
}

/*
 * Returns the level of pair in the priority queue, default is 0
 */
static uint8_t get_level(node_t **sdp_priority_queue, uint8_t number_of_levels, uint64_t key) 
{
    uint8_t current_level = 0;

    for (current_level = 0; current_level < number_of_levels; current_level++) {
        if (contains(sdp_priority_queue[current_level], key)) {
            return current_level;
        }
    }
    return 0;
}

int get_next_switch_pair(lnmp_context_t *lnmp_context, sd_pair_t *pair, cl_list_t *switch_pairs, cl_map_t **sdp_priority_queue) {
    cl_map_iterator_t sdp_itr; 
    cl_map_iterator_t sdp_end; 
    uint8_t number_of_levels = (lnmp_context->number_of_layers + 1);
    cl_list_iterator_t switch_pairs_itr = cl_list_head(switch_pairs);
    cl_list_iterator_t switch_pairs_end = cl_list_end(switch_pairs);
    uint8_t best_level = 0, current_level = 0;


    return 0;
}

/*
 * Fisher - Yates shuffle
 * end_index exclusive, start_index inclusive
 */
void randomize_switch_pairs(uint64_t *switch_pairs, uint64_t start_index, uint64_t end_index) 
{
    uint64_t i = 0, j = 0, temp = 0;
    for(i = end_index - 1; i > start_index; i--) {
        j = start_index + (rand() % (i+1 - start_index));
        temp = switch_pairs[i];
        switch_pairs[i] = switch_pairs[j];
        switch_pairs[j] = temp;
    }

}

static int generate_switch_pairs_list(lnmp_context_t *lnmp_context, uint32_t number_of_switch_pairs, node_t **sdp_priority_queue, uint64_t *switch_pairs)
{
    uint32_t adj_list_size = lnmp_context->adj_list_size;
    uint8_t layer_number = 0, lmc = 0;
    uint32_t index = number_of_switch_pairs - 1; 
    uint32_t prev_index = index;
    uint64_t key;
    boolean_t direction = FALSE; // = decreasing
    uint32_t i = 0, j = 0;

    for(layer_number = lnmp_context->number_of_layers; layer_number > 0; layer_number--) {
        add_from_offset(sdp_priority_queue[layer_number], &index, switch_pairs, direction);
        if (prev_index != index) {
            randomize_switch_pairs(switch_pairs, index + 1, prev_index + 1);
        }
        prev_index = index;
    }
    
    for (i = 1; i < adj_list_size; i++) {
        for (j = 1; j < adj_list_size; j++) {
            if (i == j)
                continue;
            key = ((uint64_t) i << 32) + (uint64_t) j;
            if (!get_level(sdp_priority_queue, lnmp_context->number_of_layers + 1, key)) //true if key is on level 0
                switch_pairs[index--] = key;
        }
    }
    
    //index should have overflown as we decrement after inserting at position 0
    if(index != (uint32_t) -1)
        goto ERROR;

    if(prev_index != index)
        randomize_switch_pairs(switch_pairs, index + 1, prev_index + 1);

    return 0;

ERROR:
    // TODO
    return -1;
}

int lnmp_generate_layer(lnmp_context_t *lnmp_context, osm_ucast_mgr_t *p_mgr, uint8_t layer_number, node_t **sdp_priority_queue, uint32_t **weights)
{
    /* TODO Check if there is need to clear the layer beforehand */

    vertex_t *layer = lnmp_context->layers[layer_number];
    uint32_t adj_list_size = lnmp_context->adj_list_size;
    uint64_t pair;
    uint64_t *switch_pairs;
    uint32_t switch_pairs_size = (adj_list_size -1) * (adj_list_size -2), added_paths = 0;
    uint32_t current_switch_pair = 0;
    uint32_t i = 0, j = 0;

    switch_pairs = (uint64_t *) calloc(switch_pairs_size, sizeof(uint64_t));
    if (!switch_pairs) {
        goto ERROR;
    }
    
    if(!generate_switch_pairs_list(lnmp_context, switch_pairs_size, sdp_priority_queue, switch_pairs))
        goto ERROR;
    

    while(current_switch_pair < switch_pairs_size && added_paths < lnmp_context->maximum_number_of_paths) {
        pair = switch_pairs[current_switch_pair++]; 


    }


    return 0;
ERROR:
    // TODO
    return -1;
}

static int lnmp_perform_routing(void *context)
{
    lnmp_context_t *lnmp_context = (lnmp_context_t *) context;
    osm_ucast_mgr_t *p_mgr = (osm_ucast_mgr_t *) lnmp_context->p_mgr;
    vertex_t *adj_list = (vertex_t *) lnmp_context->adj_list;
    uint32_t adj_list_size = lnmp_context->adj_list_size;
    uint32_t **weights;
    uint32_t i = 0;
    uint8_t layer_number = 0, lmc = 0;
    uint16_t min_lid_ho = 0;

    cl_qmap_t *sw_tbl = &p_mgr->p_subn->sw_guid_tbl;
    cl_map_item_t *item = NULL;
    osm_switch_t *sw = NULL;

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

    /* reset link wights */

    node_t **sdp_priority_queue = NULL; /* array that uses the level as index and points to sorted binary trees (node_t), which in turn go from added paths according to first_base_lid concat second_base_lid to their usage in the current layer*/
    sdp_priority_queue = (node_t **) calloc(lnmp_context->number_of_layers + 1, sizeof(node_t *));
    if (!sdp_priority_queue) {
        OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                "ERR AD02: cannot allocate memory for priority queue switch pairs\n");
        goto ERROR;
    }
    //allocate weight_matrix and initialize to zero, remember that the first position in adj_list is reserved

    weights = (uint32_t **) malloc((adj_list_size-1) * sizeof(uint32_t *));
    if(!weights) {
        OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                "ERR AD02: cannot allocate memory for weight matrix\n");
        goto ERROR;
    }

    for(i = 0; i < adj_list_size -1; i++) {
        weights[i] = (uint32_t *) calloc((size_t) (adj_list_size - 1), sizeof(uint32_t));
        if(!weights[i]) {
            OSM_LOG(p_mgr->p_log, OSM_LOG_ERROR,
                    "ERR AD02: cannot allocate memory for column in weight matrix\n");
            goto ERROR;
        }
    }

    /* generate layers */

    for(layer_number = 0; layer_number < lnmp_context->number_of_layers; layer_number++) {
        lnmp_generate_layer(lnmp_context, p_mgr, layer_number, sdp_priority_queue, weights);
    }

ERROR:
    //TODO clean up
    return -1;
}

static uint8_t get_lnmp_sl(void *context, uint8_t hint_for_default_sl, const ib_net16_t slid, const ib_net16_t dlid)
{
    return hint_for_default_sl;
}


int osm_ucast_lnmp_setup(struct osm_routing_engine *r, osm_opensm_t *p_osm)
{
    lnmp_context_t *lnmp_context = lnmp_context_create(p_osm, OSM_ROUTING_ENGINE_TYPE_LNMP);
    if(!lnmp_context) {
        return 1; /* alloc failed -> skip this routing */
    }

    /* reset funciton pointer to lnmp routines */
    r->context = (void *)lnmp_context;
    r->build_lid_matrices = lnmp_build_graph;
    //TODO
    //r->ucast_build_fwd_tables = dfsssp_do_dijkstra_routing;
    //r->mcast_build_stree = dfsssp_do_mcast_routing;

    //r->path_sl = get_lnmp_sl; could potentially assign this when determining the paths per layer
    r->destroy = delete;

    /* we initialize with the current time to achieve a 'good' randomized
       assignment in get_dfsssp_sl(...)
       */
    srand(time(NULL));

    return 0;
}
