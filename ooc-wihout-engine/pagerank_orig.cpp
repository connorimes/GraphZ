#include<iostream>
#include<fstream>
#include "io_task.hpp"
#include "common.h"
#include "GraphProperty.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/progress.hpp"

namespace boostfs = boost::filesystem;
//const string mode = "1par"; //the mem is large enough to hold all vertices' values
const string mode = "multi-par"; //the mem cannot hold all vertices' values and it will have multiple partitions
const float prob = 0.15;

struct vertex_val_t{
    float vval;
    float agg_msg;
    
    vertex_val_t(){
        vval= 1.0 ;
        agg_msg=0;
    }
};

vertex_val_t *vertices; //store all vertices' values and accumlated messages

GraphProperty *gp;
fstream *fbases;
fstream *fedges;
vector<fstream *> fmsgs;

vertex_id globalv2localv(vertex_id gid);
vertex_id localv2globalv(vertex_id lid, int nth_par);

void init_global(string _fpath){
    gp = new GraphProperty(_fpath);
    if( mode.compare("1par") == 0){
        return; 
    } //init those message filestreams
    fmsgs.reserve(gp->par_num);
    for(int nth_par=0; nth_par < gp->par_num; nth_par++){
        string ops_path =  gp->get_par_path(nth_par) + string("val.ops");
        fmsgs[nth_par] = new fstream( ops_path.c_str(), ios::in | ios::out | ios::trunc );
        if(!fmsgs[nth_par]->good()){
            cerr << "open file error in init_global" << endl;
            exit(-1);
        }
    }
}

void apply_message(vertex_id vid, float msg){
    vertices[globalv2localv(vid)].agg_msg += msg;
}

void ftruncate(fstream *fs, int nth_par){
    fs->clear();
    fs->seekg(0, fs->beg);
    fs->seekp(0, fs->beg);
    boost::filesystem::detail::resize_file(\
        boost::filesystem::path((gp->get_par_path(nth_par) + string("val.ops")).c_str()), 0);
}

void init_per_par(int iter, int nth_par){
    string base_path = gp->get_par_path(nth_par) + string("bases.idx");
    string edge_path = gp->get_par_path(nth_par) + string("output.graph");
    fbases = new fstream(base_path.c_str());
    fedges = new fstream(edge_path.c_str());
    if(!fbases->good() || !fedges->good()){
        cerr << "open file error in init_per_par" << endl;
        exit(-1);
    }
    if( mode.compare("1par") == 0){
        return;
    }
    if(iter == 0){
        vertex_val_t init_vval;
        for(vertex_id i =0; i<gp->nvertices_per_partition; i++){
            vertices[i] = init_vval;
        }
    }else{
        ifstream fvalr((gp->get_par_path(nth_par) + string("/val")).c_str());
        fvalr.read((char *)vertices, sizeof(vertex_val_t) * gp->nvertices_per_partition);
        if(fvalr.gcount() != sizeof(vertex_val_t) * gp->nvertices_per_partition){
            cerr << "read error when init/restore vertices vals" << endl;
            cerr << "fvalr->gcount(): " << fvalr.gcount() << endl;
            cerr << "sizeof(vertex_val_t) * gp->nvertices_per_partition: " \
                 << sizeof(vertex_val_t) * gp->nvertices_per_partition << endl;
            exit(-1);
        }
        vertex_id vid;
        float msg;

        fmsgs[nth_par]->seekg(0, fmsgs[nth_par]->beg);
        while(!fmsgs[nth_par]->eof()){
            //cerr << "fmsgs[nth_par]->tellg(): " << fmsgs[nth_par]->tellg() << endl;
            fmsgs[nth_par]->read((char *)&vid, sizeof(vertex_id));
            if(fmsgs[nth_par]->gcount() != sizeof(vertex_id)){
                //cerr << "get out" << endl;
                //cerr << "fmsgs[nth_par]->gcount(): " << fmsgs[nth_par]->gcount() << endl;
                cerr << "fmsgs[nth_par]->tellg(): " << fmsgs[nth_par]->tellg() << endl;
                break;
            }
            fmsgs[nth_par]->read((char *)&msg, sizeof(float));
            apply_message(vid, msg);
        }
        ftruncate(fmsgs[nth_par], nth_par);
    }
}

//only usefull for 1par processing
void reset_per_par(){
    fbases->clear();
    fedges->clear();
    fbases->seekg(0, fbases->beg);
    fedges->seekg(0, fedges->beg);
}

void save_vals(int nth_par){
    cout << gp->get_par_path(nth_par) << endl;
    string val = gp->get_par_path(nth_par) + string("/val");
    ofstream fval(val.c_str());
    //fval.write((char *)vertices, sizeof(vertex_val_t) * gp-> vertices_num);
    fval.write((char *)vertices, sizeof(vertex_val_t) * gp->nvertices_per_partition);
    fval.close();
    if( mode.compare("1par") == 0){
        delete[] vertices;
    }
 
}

void clear_per_par(){
    fbases->close();
    fedges->close();
    delete fbases;
    delete fedges;
}

bool is_vertex_in_par(vertex_id vid, int nth_par){
    if(vid/gp->nvertices_per_partition == nth_par)
        return true;
    return false;
}

vertex_id globalv2localv(vertex_id gid){
    return gid % gp->nvertices_per_partition;
}

vertex_id localv2globalv(vertex_id lid, int nth_par){
    return lid + nth_par * gp->nvertices_per_partition;
}

void write_message(vertex_id vid, float msg){
    int target_par = vid / gp->nvertices_per_partition;
    //cerr << "target_par: " << target_par << endl;
    fmsgs[target_par]->write((char*)&vid, sizeof(vertex_id));
    fmsgs[target_par]->write((char*)&msg, sizeof(float));
}

template<typename edge_t>
inline void process_vertex(graphzx::adjlst<edge_t> *adj, int iter, int nth_par=0){
    if(iter == 0){
         vertices[globalv2localv(adj->fid)].vval = prob;
         for(counter i=0; i< adj->num; i++){
             if(is_vertex_in_par(adj->edges[i], nth_par))
                 vertices[globalv2localv(adj->edges[i])].agg_msg += 1.0/adj->num;
             else
                 write_message(adj->edges[i], 1.0/adj->num);
         }
    }else{
        float pr = prob + vertices[globalv2localv(adj->fid)].agg_msg * (1-prob);
        vertices[globalv2localv(adj->fid)].vval = pr;
        vertices[globalv2localv(adj->fid)].agg_msg = 0.0;
        float vote = pr/adj->num;
        for(counter i=0; i< adj->num; i++){
            if(is_vertex_in_par(adj->edges[i], nth_par))
                vertices[globalv2localv(adj->edges[i])].agg_msg += vote;
            else
                write_message(adj->edges[i], vote);
        }
    }
}

//used for when there is only 1 partition
void iter1par(int iter, int nth_par=0){
    unsigned long long last_ofst = 0;
    unsigned long long cur_ofst = 0;
    fbases->read((char *)&cur_ofst, sizeof(unsigned long long));
    while(fbases->gcount() == sizeof(unsigned long long)){
        //cout << last_ofst << " " << cur_ofst << endl;
        unsigned int num_neighbor = 0;
        if( cur_ofst != last_ofst){
            num_neighbor = (cur_ofst-last_ofst)/sizeof(vertex_id);
        }
        //cout << num_neighbor << endl;
        last_ofst = cur_ofst;
        vertex_id lid = fbases->tellg()/sizeof(unsigned long long) - 1;
        graphzx::adjlst<vertex_id> *adj = \
            graphzx::adjlst<vertex_id>::get_adjlst(\
            localv2globalv(lid, nth_par), num_neighbor);
        if(!adj){
           cerr << "error when getting vertices" << endl;
           exit(-1);
        }
        //cout << sizeof(vertex_id) * num_neighbor << endl;
        //cout << fedges->tellg() << endl;
        fedges->read((char *)(adj->edges), sizeof(vertex_id) * num_neighbor );
        //adj->tell_self();
        process_vertex(adj, iter, nth_par); 
        graphzx::adjlst<vertex_id>::free_adjlst(adj);
        //adj->tell_self();
        //cout << "good?" << fbases->good() << endl;
        //cout << fbases->tellg() << endl;
        fbases->read((char *)&cur_ofst, sizeof(unsigned long long));
    }
}

void process1par(int max_iter){
    cout << "start processing in-mem" << endl;
    init_per_par(0, 0);
    vertices = new vertex_val_t[gp->vertices_num];
    for(int nth_iter = 0; nth_iter<max_iter; nth_iter++){
        //cout << "[INFO]iteration: " << nth_iter << endl;
        iter1par(nth_iter);
        reset_per_par();
    }
    save_vals(0);
    clear_per_par();
}

//used for when there are multiple partitions
void iter_nth_par(int max_iter){
}

void process_multi_par(int max_iter){
    cout << "start processing out-of-core" << endl;
    vertices = new vertex_val_t[gp->nvertices_per_partition];
    for(int nth_iter = 0; nth_iter<max_iter; nth_iter++){
        cout << "[INFO]iteration: " << nth_iter << endl;
        for(int nth_par = 0; nth_par<gp->par_num; nth_par ++){
            cout << "[INFO]partition: " << nth_par << endl;
            init_per_par(nth_iter, nth_par);
            //iter_nth_par(nth_iter);
            iter1par(nth_iter, nth_par);
            save_vals(nth_par);
            clear_per_par();
        }
    }
}

void process(string fpath, int max_iter){
    init_global(fpath);
    if( mode.compare("1par") == 0){
        process1par(max_iter);
    }else if( mode.compare("multi-par") == 0){
        process_multi_par(max_iter); 
    }
}

int main(int argc, char *argv[]){
    string fpath = argv[1];
    //string fpath = "/run/media/xu/graph/noengine/data/com-lj.ungraph.txt";
    //string fpath = "/run/media/xu/graph/noengine/data/followers.txt";
    int max_iter = 6;
    process(fpath, max_iter);    
}
