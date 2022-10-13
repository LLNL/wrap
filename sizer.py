import json

def singleelem(args):
    ret = """
    size_t _size_{} = 0;
    {{
        MPI_Count _tsize = 0;
        PMPI_Type_size_x({}, &_tsize);
        _size_{} = _tsize;
    }}
    """.format(args["dir"], args["type"], args["dir"])

    return ret

def singlevalue(args):
    ret = """
    size_t _size_{} = 0;
    {{
        _size_{} = {};
    }}
    """.format(args["dir"], args["dir"], args["arg"])

    return ret


def typecount(args):
    ret = """
    size_t _size_{} = 0;
    {{
""".format(args["dir"])

    if "root" in args:

        comp = "!" if "notroot" in args else "="
        ret += """
        int _rank = 0;
        PMPI_Comm_rank({}, &_rank);
        if( _rank {}= {})""".format( args["comm"], comp,  args["root"])


    ret += """
        {{
            MPI_Count _tsize = 0;
            PMPI_Type_size_x({}, &_tsize);
            _size_{} = {} * _tsize;
        }}
    }}
    """.format(args["type"], args["dir"], args["count"])

    return ret

def withall(args):
    ret = """
    size_t _size_{} = 0;
    {{
""".format(args["dir"])
    if "root" in args:
        ret += """
    int _rank = 0;
    PMPI_Comm_rank({}, &_rank);
    if( _rank == {})
""".format( args["comm"],  args["root"])

    ret += """
        {{
            MPI_Count _tsize = 0;
            PMPI_Type_size_x({}, &_tsize);
            int _csize = 0;
    """.format(args["type"])

    if "isneigh" in args:
        ret += "    _csize = topo_neigh_count({});\n".format(args["comm"])
    else:
        ret += "    PMPI_Comm_size({}, &_csize);\n".format(args["comm"])

    ret +=  """
            _size_{} = {} * _tsize * _csize;
        }}
    }}
    """.format(args["dir"], args["count"])

    return ret

def withallv(args):
    ret = """
    size_t _size_{} = 0;
    {{
""".format(args["dir"])
    if "root" in args:
        ret += """
    int _rank = 0;
    PMPI_Comm_rank({}, &_rank);
    if( _rank == {})
""".format( args["comm"],  args["root"])

    ret += """
        {{
            MPI_Count _tsize = 0;
            PMPI_Type_size_x({}, &_tsize);
            int _csize = 0;
        """.format(args["type"])

    if "isneigh" in args:
        ret += "    _csize = topo_neigh_count({});\n".format(args["comm"])
    else:
        ret += "    PMPI_Comm_size({}, &_csize);\n".format(args["comm"])


    ret += """
            size_t _total_count = 0;
            int i;
            for(i = 0 ; i < _csize; i++)
            {{
                _total_count += {}[i];
            }}
            _size_{} = _total_count * _tsize;
        }}
    }}
    """.format(args["allcounts"],
               args["dir"])

    return ret


def withallw(args):
    ret = """
    size_t _size_{} = 0;
    {{
""".format(args["dir"])

    if "root" in args:
        ret += """
    int _rank = 0;
    PMPI_Comm_rank({}, &_rank);
    if( _rank == {})
""".format( args["comm"],  args["root"])

    ret += """
        {
            int _csize = 0;
        """

    if "isneigh" in args:
        ret += "    _csize = topo_neigh_count({});\n".format(args["comm"])
    else:
        ret += "    PMPI_Comm_size({}, &_csize);\n".format(args["comm"])


    ret += """
            size_t _total_size = 0;
            int i;
            for(i = 0 ; i < _csize; i++)
            {{
                MPI_Count _tsize = 0;
                PMPI_Type_size_x({}[i], &_tsize);
                _total_size += {}[i] * _tsize;
            }}
            _size_{} = _total_size;
        }}
    }}
    """.format(args["alltypes"],
               args["allcounts"],
               args["dir"])

    return ret

MPI_SIZE_OUT = {
    "MPI_Acumulate" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Send" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
   "MPI_Isend" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Sendrecv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
   "MPI_Sendrecv_replace" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Rsend" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Allgather" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Iallgather" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
   "MPI_Neighbor_allgather" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
   "MPI_Ineighbor_allgather" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Allgatherv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Iallgatherv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
   "MPI_Ineighbor_allgatherv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
   "MPI_Ineighbor_allgatherv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Allreduce" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Iallreduce" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Alltoall" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Ialltoall" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Neighbor_alltoall" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Ineighbor_alltoall" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Alltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 1,  "type" : 3, "comm": 8}
    },
    "MPI_Ialltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 1,  "type" : 3, "comm": 8}
    },
    "MPI_Neighbor_alltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 1,  "type" : 3, "comm": 8, "isneigh" : 1}
    },
    "MPI_Ineighbor_alltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 1,  "type" : 3, "comm": 8, "isneigh" : 1}
    },
    "MPI_Alltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 1, "alltypes" : 3, "comm": 8}
    },
    "MPI_Ialltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 1, "alltypes" : 3, "comm": 8}
    },
   "MPI_Neighbor_alltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 1, "alltypes" : 3, "comm": 8, "isneigh" : 1}
    },
    "MPI_Ineighbor_alltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 1, "alltypes" : 3, "comm": 8, "isneigh" : 1}
    },
    "MPI_Bcast" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2, "comm" : 4, "root": 3}
    },
    "MPI_Ibcast" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2, "comm" : 4, "root": 3}
    },
    "MPI_Bsend" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Ibsend" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Compare_and_swap" : {
        "fn" : singleelem,
        "args" : {"type" : 3}
    },
    "MPI_Scan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Iscan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Exscan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Iexscan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Fetch_and_op" : {
        "fn" : singleelem,
        "args" : {"type" : 2}
    },
    "MPI_Gather" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Igather" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Gatherv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Igatherv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Get_accumulate" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Rget_accumulate" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Put" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Rput" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Raccumulate" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Reduce" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Scatter" : {
        "fn" : withall,
        "args" : {"count" : 1, "type" : 2, "root" : 6, "comm": 7}
    },
    "MPI_Iscatter" : {
        "fn" : withall,
        "args" : {"count" : 1, "type" : 2, "root" : 6, "comm": 7}
    },
    "MPI_Scatterv" : {
        "fn" : withallv,
        "args" : {"allcounts" : 1, "type" : 3, "root" : 7, "comm": 8}
    },
    "MPI_Iscatterv" : {
        "fn" : withallv,
        "args" : {"allcounts" : 1, "type" : 3, "root" : 7, "comm": 8}
    },
    "MPI_Ssend" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Issend" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_File_preallocate" : 
    {
        "fn" : singlevalue,
        "args" : { "arg" : 1}
    },
    "MPI_File_write_at" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_write_at_all" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_iwrite_at_all" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_iwrite_at" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_write" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_write_all" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_iwrite_all" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_iwrite" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_write_shared" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_iwrite_shared" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_write_ordered" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },

#Miss MPI_REDUCE_SCATTER & MPI_REDUCE_SCATTER_BLOCK
}

MPI_SIZE_IN = {
    "MPI_Allgather" : {
        "fn" : withall,
        "args" : {"count" : 4, "type" : 5, "comm": 6}
    },
    "MPI_Iallgather" : {
        "fn" : withall,
        "args" : {"count" : 4, "type" : 5, "comm": 6}
    },
    "MPI_Ineighbor_allgather" : {
        "fn" : withall,
        "args" : {"count" : 4, "type" : 5, "comm": 6, "isneigh" : 1}
    },
    "MPI_Allgatherv" : {
        "fn" : withallv,
        "args" : {"allcounts": 4,  "type" : 6, "comm": 7}
    },
    "MPI_Iallgatherv" : {
        "fn" : withallv,
        "args" : {"allcounts": 4,  "type" : 6, "comm": 7}
    },
    "MPI_Neighbor_allgatherv" : {
        "fn" : withallv,
        "args" : {"allcounts": 4,  "type" : 6, "comm": 7, "isneigh" : 1}
    },
    "MPI_Ineighbor_allgatherv" : {
        "fn" : withallv,
        "args" : {"allcounts": 4,  "type" : 6, "comm": 7, "isneigh" : 1}
    },
    "MPI_Recv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Irecv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Allreduce" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Iallreduce" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Alltoall" : {
        "fn" : typecount,
        "args" : {"count": 4,  "type" : 5}
    },
    "MPI_Alltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 5,  "type" : 7, "comm": 8}
    },
    "MPI_Ialltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 5,  "type" : 7, "comm": 8}
    },
    "MPI_Neighbor_alltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 5,  "type" : 7, "comm": 8, "isneigh" : 1}
    },
    "MPI_Ineighbor_alltoallv" : {
        "fn" : withallv,
        "args" : {"allcounts": 5,  "type" : 7, "comm": 8, "isneigh" : 1}
    },
    "MPI_Alltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 5, "alltypes" : 7, "comm": 8}
    },
    "MPI_Ialltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 5, "alltypes" : 7, "comm": 8}
    },
   "MPI_Neighbor_alltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 5, "alltypes" : 7, "comm": 8, "isneigh" : 1}
    },
    "MPI_Ineighbor_alltoallw" : {
        "fn" : withallw,
        "args" : {"allcounts": 5, "alltypes" : 7, "comm": 8, "isneigh" : 1}
    },
    "MPI_Bcast" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2, "comm" : 4, "root": 3, "notroot": True}
    },
    "MPI_Ibcast" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2, "comm" : 4, "root": 3, "notroot": True}
    },
    "MPI_Compare_and_swap" : {
        "fn" : singleelem,
        "args" : {"type" : 3}
    },
    "MPI_Exscan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Iexscan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Scan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Iscan" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_Fetch_and_op" : {
        "fn" : singleelem,
        "args" : {"type" : 2}
    },
    "MPI_Gather" : {
        "fn" : withall,
        "args" : {"count" : 4, "type" : 5, "root" : 6, "comm": 7}
    },
    "MPI_Igather" : {
        "fn" : withall,
        "args" : {"count" : 4, "type" : 5, "root" : 6, "comm": 7}
    },
    "MPI_Gatherv" : {
        "fn" : withallv,
        "args" : {"allcounts" : 4, "type" : 6, "root" : 7, "comm": 8}
    },
    "MPI_Igatherv" : {
        "fn" : withallv,
        "args" : {"allcounts" : 4, "type" : 6, "root" : 7, "comm": 8}
    },
    "MPI_Get" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Rget" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Get_accumulate" : {
        "fn" : typecount,
        "args" : {"count" : 4, "type" : 5}
    },
    "MPI_Rget_accumulate" : {
        "fn" : typecount,
        "args" : {"count" : 4, "type" : 5}
    },
    "MPI_Imrecv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Rrecv" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_Reduce" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3, "root": 5, "comm": 6}
    },
   "MPI_Scatter" : {
        "fn" : typecount,
        "args" : {"count" : 4, "type" : 5}
    },
   "MPI_Iscatter" : {
        "fn" : typecount,
        "args" : {"count" : 4, "type" : 5}
    },
   "MPI_Scatterv" : {
        "fn" : typecount,
        "args" : {"count" : 5, "type" : 6}
    },
   "MPI_Iscatterv" : {
        "fn" : typecount,
        "args" : {"count" : 5, "type" : 6}
    },
    "MPI_Sendrecv" : {
        "fn" : typecount,
        "args" : {"count" : 6, "type" : 7}
    },
   "MPI_Sendrecv_replace" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
   "MPI_Sendrecv_replace" : {
        "fn" : typecount,
        "args" : {"count" : 1, "type" : 2}
    },
    "MPI_File_iread_at" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_read" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_read_all" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_iread_all" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_read_at" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_read_at_all" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_iread_at_all" : {
        "fn" : typecount,
        "args" : {"count" : 3, "type" : 4}
    },
    "MPI_File_iread" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_read_shared" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_iread_shared" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
    "MPI_File_read_ordered" : {
        "fn" : typecount,
        "args" : {"count" : 2, "type" : 3}
    },
}

class MpiCallSize():

    _graph_count_code_c = """
#include <stdlib.h>
#include <stddef.h>


int cart_info_keyval_delete(MPI_Comm comm, int keyval, void *attribute_val, void *extra_state)
{
    free(attribute_val);
    return MPI_SUCCESS;
}

int cart_info_keyval_dup(MPI_Comm oldcomm, int keyval, void *extra_state,
                         void *attribute_val_in, void *attribute_val_out, int *flag)
{
    int * new = malloc(sizeof(int));
    *new = *((int *)attribute_val_in);

    *flag = 1;

    *( (void **)attribute_val_out) = new;

    return MPI_SUCCESS;
}

static inline int _mpi_cart_neigbor_count(MPI_Comm cart_comm)
{
    static int cart_info_keyval = -1;


    int neigh_count = 0;

    int * neigh = NULL;
    int flag = 0;

    if(cart_info_keyval != -1)
    {
        PMPI_Comm_get_attr( cart_comm, cart_info_keyval , &neigh, &flag);
    }

    if(!flag)
    {

        if(cart_info_keyval == -1)
        {
            PMPI_Comm_create_keyval(cart_info_keyval_dup, cart_info_keyval_delete, &cart_info_keyval, NULL);
        }

        int ndim;
        int r, d, dir;

        int my_rank;
        PMPI_Comm_rank(cart_comm, &my_rank);
        PMPI_Cartdim_get(cart_comm, &ndim);


        for(d = 0 ; d < ndim; d++)
        {
            for(dir = -1 ; dir <= 1 ; dir += 2)
            {
                int source;
                int dest;

                PMPI_Cart_shift(cart_comm, d, dir, &source, &dest);

                if(dest != MPI_PROC_NULL)
                {
                    neigh_count++;
                }
            }
        }

        neigh = malloc(sizeof(int));

        if(!neigh)
        {
            perror("malloc");
            return -1;
        }

        *neigh = neigh_count;

        PMPI_Comm_set_attr( cart_comm , cart_info_keyval , (void*)neigh);

    }
    else
    {
        if(neigh)
        {
            neigh_count = *neigh;
        }
    }

    return neigh_count;
}

static int topo_neigh_count(MPI_Comm comm)
{
    int ret = 0;

    int topo, rank;

    MPI_Topo_test( comm , &topo);

    switch (topo)
    {
    case MPI_GRAPH:
    case MPI_DIST_GRAPH:
        PMPI_Comm_rank(comm, &rank);
        MPI_Graph_neighbors_count( comm, rank, &ret);
        break;
    case MPI_CART:
        ret = _mpi_cart_neigbor_count(comm);
    default:
        break;
    }

    return ret;
}
"""
    def header_code(self):
        return self._graph_count_code_c

    def _check_args(self, args):
        for e in args:
            if not isinstance(e, str):
                raise Exception("Only string argument names are supported")

    def __init__(self, lang="c"):
        self.lang = lang


    def _unfold(self, dir, fn, args):
        self._check_args(args)

        if dir == "in":
            ref = MPI_SIZE_IN
        elif dir == "out":
            ref = MPI_SIZE_OUT
        else:
            raise Exception("Dir is either in or out")

        if fn not in ref:
            return "    size_t _size_{} = 0;\n".format(dir)

        # Prepare args
        ctx = ref[fn]

        ctx_args = {}

        for k,v in ctx["args"].items():
            ctx_args[k] = args[v]

        ctx_args["dir"] = dir

        return (ctx["fn"])(ctx_args)



    def sizein(self, fn, args):
        return self._unfold("in", fn, args)


    def sizeout(self, fn, args):
        return self._unfold("out", fn, args)

    def size(self, fn, args):
        return self.sizein(fn, args) + self.sizeout(fn, args) + "\n    size_t _size = _size_in + _size_out;"
