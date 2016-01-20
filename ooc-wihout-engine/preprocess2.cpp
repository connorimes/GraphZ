#include<iostream>
#include<fstream>
#include <string.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<queue>
//#include"../inc/Bitmap.hpp"
#include "../inc/graphtypes.h"
#include "stxxl/sorter"
#include "stxxl/queue"
//#include <stxxl/vector>
#include "stxxl.h"


#include "../inc/pre_process.h"
#include "../inc/logger.hpp"
#include <libconfig.h++> 

#define NODE_NUM 5997962
#define MAX_EDGES 20000
#define BUF_SIZ 100000000
#define INF ((unsigned int)-1)
namespace pre {
    using namespace std;
    using namespace libconfig;

    //stxxl::vector<fod_edge_t> od_edges;
    //stxxl::vector<fod_edge_t, 1, stxxl::lru_pager<16>, (10*1024*1024), stxxl::RC, stxxl::uint64> od_edges;

    typedef stxxl::sorter<fedge_t, fedge_comparator, 2 * 1024 * 1024> fedge_sorter_t;
    typedef stxxl::sorter<fod_edge_t, fod_edge_degree_comparator, 2 * 1024 * 1024> fod_edge_degree_sorter_t;
    typedef stxxl::sorter<fod_edge_t, fod_edge_fid_comparator, 2 * 1024 * 1024> fod_edge_fid_sorter_t;
    typedef stxxl::sorter<fod_edge_t, fod_edge_tid_comparator, 2 * 1024 * 1024> fod_edge_tid_sorter_t;
    
    fod_edge_fid_sorter_t od_edges_init(fod_edge_fid_comparator(), 640*1024*1024); //use for reading
 //   fod_edge_fid_sorter_t od_edges_init_degrees(fod_edge_fid_comparator(), 64*1024*1024); //use for get degrees
    stxxl::queue<fod_edge_t> od_edges_init_degrees;
 //   std::queue<fod_edge_degree_sorter_t *> partitions_relabel_fids; //use for partitioning and relabel fids
    fod_edge_tid_sorter_t od_edges_relabel_tids(fod_edge_tid_comparator(), 640*1024*1024); //use for sorting and then relabel tids
    fod_edge_fid_sorter_t od_edges_relabeled(fod_edge_fid_comparator(), 640*1024*1024); //use to store edges that are relabled completely
    
    unsigned long long nvertices_per_partition;

   char *ifpath = "/home/ubu/Downloads/com-lj.ungraph.txt.100000";
 //   char *ifpath = "/home/ubu/Downloads/com-lj.ungraph.txt";
 //   char *ifpath = "/run/media/xu/graph/ydata-sum.txt";
    //char *ifpath = "/home/xu/Downloads/friend/com-friendster.ungraph.txt";
  //  char *ifpath = "/mnt/ssd/friend/com-friendster.ungraph.txt";

    bool directed = true;
    //set both of them to 1, ignore the useless condition
    unsigned long long active_vertices_num = 1; //the number of vertices that has a out-edge
    unsigned long long vertices_num = 1; //number of all vertices

    char abs_path[200]={0};
    string root_dir;
    char line[200];

    string ofpath;
    string base_path;
    string idbase_path;
    string lookup_path;
    string lookup_pathr;
    string config_path;

    ifstream *fin = NULL;
    ofstream *fout = NULL; unsigned long long out_ofst = 0;
    ofstream *foutn = NULL;
    ofstream *fbase_out = NULL;
    ofstream *fidbase_out = NULL;
    fstream *flookup_out = NULL;
    fstream *flookup_outr = NULL;

    unsigned long long edges_idx = 0;
    //use the global variable root_dir and realpath

    vertex_id ididx = 0;
    vertex_id newid0 = 0;
    offset base = 0;
    unsigned int partition_num; //the number of partitions
    
    // graphzx::Bitmap *bmp;

    vertex_id label_buf[BUF_SIZ];

    void newdir(string dir) {
        int status;
        status = mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }

    void newfile(string f) {
        string cmd("touch ");
        cmd += f;
        system(cmd.c_str());
    }

    void cleanup_per_partition() {
        fout->close();
        fbase_out->close();
        fidbase_out->close();

        delete fout;
        delete fbase_out;
        delete fidbase_out;
    }

    void init_pathes(unsigned int nth_partition) {
        char tmp[10];
        sprintf(tmp, "%d", nth_partition);
        string nth(tmp);
        newdir(root_dir + nth + string("/"));
        ofpath = root_dir + nth + string("/output.graph");
        base_path = root_dir + nth + string("/bases.idx");
        idbase_path = root_dir + nth + string("/idbases.idx");
    }

    void init_global() {
        lookup_path = root_dir + string("lookup.map");
        lookup_pathr = root_dir + string("lookup.mapr");
        newfile(lookup_path);
        newfile(lookup_pathr);
        flookup_out = new fstream(lookup_path.c_str(), ios::in | ios::out | ios::binary | ios::trunc);
        flookup_outr = new fstream(lookup_pathr.c_str(), ios::in | ios::out | ios::binary | ios::trunc);
        config_path = root_dir + string("/properties.cfg");
    }

    void initstreams() {
        fout = new ofstream(ofpath.c_str(), ios::out | ios::binary | ios::trunc);
        fbase_out = new ofstream(base_path.c_str(), ios::out | ios::binary | ios::trunc);
        fidbase_out = new ofstream(idbase_path.c_str(), ios::out | ios::binary | ios::trunc);
    }

    bool init_partition(unsigned int nth_partition) {
        init_pathes(nth_partition);
        initstreams();
    }

    void init_root_dir() {
        string tmp(abs_path);
        root_dir = tmp + string(".nodos_dir/");
        newdir(root_dir);
    }

    void update_vertices_nums(vertex_id fid, vertex_id tid){
        vertex_id maxid = std::max(fid, tid);
        active_vertices_num = 1 + std::max(active_vertices_num-1, (unsigned long long)fid);
        vertices_num = 1 + std::max(vertices_num-1, (unsigned long long)maxid);
    }

    void read_edges() {
        char *p;
        while (!fin->eof()) {
            fin->getline(line, 200);
            if (line[0] >= '0' && line[0] <= '9') {
                fod_edge_t aedge;
                p = strtok(line, " \t\r");
                aedge.fid = atoll(p);
                p = strtok(NULL, " \t\r");
                aedge.tid = atoll(p);
                od_edges_init.push(aedge);
                update_vertices_nums(aedge.fid, aedge.tid);
                if (!directed) {
                    fod_edge_t aredge;
                    aredge.fid = aedge.tid;
                    aredge.tid = aedge.fid;
                    od_edges_init.push(aredge);
                }
            }
        }
        od_edges_init.sort();
    }

    bool init(char *ifpath) {
        //realpath(ifpath, abs_path);
        memcpy(abs_path, ifpath, strlen(ifpath)+1);
        init_root_dir();
        init_global();
        fin = new ifstream();
        fin->open(ifpath);
        cout << "abs_path: " << abs_path << endl;
        cout << "root_dir: " << root_dir << endl;

        read_edges();
        return true;
    }

    void get_vertices_num() {
        vertices_num = flookup_outr->tellp() / sizeof (vertex_id);
        logstream(LOG_INFO) << "vertices_num: " << vertices_num << std::endl;
    }

    bool process_partition(unsigned int nth_partition) {
        std::cout << "start nth_partition: " << nth_partition<< std::endl;
        init_partition(nth_partition); 
        out_ofst = 0;
        
        for(vertex_id id = nth_partition * nvertices_per_partition; \
                id < (nth_partition + 1) * nvertices_per_partition && \
                id < vertices_num; id++){
            fod_edge_t aedge = fod_edge_t(-1, -1, -1);
            if(!od_edges_init.empty())
                aedge = *od_edges_init;

            //std::cout << "aedge: " << aedge.fid << "," << aedge.tid << std::endl;

//            fbase_out->write((char *) &(out_ofst), sizeof (unsigned long long));
            while (id == aedge.fid && !od_edges_init.empty()) {
//                std::cout << "aedge: " << aedge.fid << "," << aedge.tid << std::endl;
                fout->write((char *) &(aedge.tid), sizeof (vertex_id));
                out_ofst += sizeof (vertex_id);
                ++od_edges_init;
                if (!od_edges_init.empty())
                    aedge = *od_edges_init;
            }
            //std::cout << "id: " << id << "\t out_ofst: " << out_ofst << std::endl;
            fbase_out->write((char *) &(out_ofst), sizeof (unsigned long long));
        }
        
        cleanup_per_partition();
        
        std::cout << "stop nth_partition: " << nth_partition<< '\n' << std::endl;
        if ((nth_partition + 1) * nvertices_per_partition >= vertices_num) {
            od_edges_init.clear();
            return true;
        }
        return false;
    }

    void save_configs() {
        libconfig::Config cfg;
        //cfg.readFile(config_path);
        libconfig::Setting& root = cfg.getRoot();
        root.add("graph", Setting::TypeGroup);
        libconfig::Setting& graph = root["graph"];
        graph.add("active_vertices_num", Setting::TypeInt64) = (const long long )active_vertices_num;
        graph.add("vertices_num", Setting::TypeInt64) = (const long long )vertices_num;
        graph.add("partition_num", Setting::TypeInt64) =(const long long ) partition_num;
        graph.add("nvertices_per_partition", Setting::TypeInt64) =(const long long ) nvertices_per_partition;
        cfg.writeFile(config_path.c_str());
    }

    void preprocess(char *ifpath, unsigned long long partition_size, bool _directed) {
        nvertices_per_partition = partition_size;
        directed = _directed;
        init(ifpath);
        
       // get_vertices_num();
        partition_num = 0;
        while (!process_partition(partition_num)) {
            partition_num++;
        }
        partition_num++;
        save_configs();
        flookup_out->close();
        flookup_outr->close();
        delete flookup_out;
        delete flookup_outr;
        
        
 
        std::cout << "parition_num: " << partition_num << std::endl;
    }
};

using namespace pre;

int main(int argc, char *argv[]) {
    //system("rm -rf /Downloads/com-lj.ungraph.txt.1000.dir/");
    system("rm /var/tmp/stxxl");
     ifpath = argv[1]; 
     unsigned long vertices_per_num = atol(argv[2]);
//     preprocess(ifpath, 1000000, true);
  //  preprocess(ifpath, 2000000, true);
 //   preprocess(ifpath, 300000000, true);
//      preprocess(ifpath, 10000000, true);
      preprocess(ifpath, vertices_per_num, true);
 //   preprocess(ifpath, 1000000, true);

    return 0;
}
