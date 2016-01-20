#ifndef IO_TASK_HPP
#define IO_TASK_HPP

#include "common.h"
#include "boost/thread/mutex.hpp"


namespace graphzx {
#define QD 64

    template<typename edge_t>
    struct adjlst {
        vertex_id fid;
        counter num; //the number of out edges
        edge_t *edges;

    public:
        static struct adjlst<edge_t>* get_adjlst(vertex_id fid, counter num) {
            struct adjlst<edge_t> *adjl = (struct adjlst<edge_t> *)malloc(sizeof ( struct adjlst<edge_t>));
            if (!adjl) {
                return NULL;
            }
            adjl->edges = (edge_t *) malloc(sizeof (edge_t) * num);
            if (!(adjl->edges)) {
                free(adjl);
                return NULL;
            }
            adjl->fid = fid;
            adjl->num = num;
            return adjl;
        }

        static struct adjlst<edge_t> free_adjlst(struct adjlst<edge_t>* adjl) {
            free(adjl->edges);
            free(adjl);
        }

    public:
        
        void tell_self(){
            std::cout << "adj: " << fid << " " << num << std::endl;
            for(int i=0; i<num; i++)
                std::cout << edges[i] << " ";
            std::cout << std::endl;
        }
    };
}
#endif

