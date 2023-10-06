A High-Performance Design, Implementation, Deployment, and Evaluation of The Slim Fly Network
=============================================================================================

## Description
This fork of OpenSM introduces the LNMP (Layered Non-Minimal Path) Routing algorithm, designed to specifically enhance the performance of low-diameter networks such as Slim Fly (SF). This routing algorithm is a part of our broader study on deploying Slim Fly networks in practice. All details on its design, applicability and performance, are detailed in our paper, ["A High-Performance Design, Implementation, Deployment, and Evaluation of The Slim Fly Network"](https://arxiv.org/pdf/2310.03742.pdf).


## Acknowledgment of Original Work
This project is based on [OpenSM](https://github.com/linux-rdma/opensm) which provides an implementation for an InfiniBand Subnet Manager and Administrator. We have extended it to include the LNMP routing algorithm.

## New Features & Modifications
- Introduced the LNMP (Layered Non-Minimal Paths) routing algorithm.
- Add best-effort deadlock removal for the DFSSSP routing algorithm.

## Configuration Parameters for LNMP

- `lnmp_max_num_paths`: Sets the maximum number of paths to be used by LNMP routing for each routing layer. Defaults to 0, which results in a maximum number of `100000` paths per layer.
- `layers_remove_deadlocks`: If set, in LNMP Routing deadlocks will be removed using the DFSSSP's deadlock resolution algorithm. If `not set` (default), deadlocks will be removed using the new deadlock removal algorithm introduced in the paper that works only for paths of length <= 3.
- `dfsssp_best_effort`: If set, DFSSSP's deadlock resolution will attemt to resolve all deadlocks, but if unsuccessful leave all extra paths in the last VL. Defaults to `not set`, which results in a crash if DFSSSP is unable to resolve all deadlocks.
- `lnmp_min_path_len`: Sets the minimum length each path that is a added to a layer needs to have. This constraint is not applied to the first layer, which is always routed minimally. Defaults to `2`, the diameter of SF MMS topologies.
- `lnmp_max_path_len`: Sets the maximum length each path that is a added to a layer is allowed to have. Defaults to `3`, one hop longer than the diameter of SF MMS topologies.


## Activate LNMP Routing
To activate and use LNMP routing on your system, simply set the routing_engine parameter in the OpenSM configuration file to `lnmp`, configure the number of layers and choose your deadlock resolution algorithm. For example, in `/etc/opensm/opensm.conf`:
```
routing_engine lnmp
lmc 2
layers_remove_deadlocks FALSE
```

## Paper Reference
For a comprehensive understanding of the LNMP routing algorithm and its performance implications, please refer to our [paper](https://arxiv.org/pdf/2310.03742.pdf). If you use the LNMP algorithm in your work, kindly cite our [paper](https://arxiv.org/pdf/2310.03742.pdf) using the following BibTeX entry:
```bibtex
@misc{blach2023highperformance,
    title = {{A High-Performance Design, Implementation, Deployment, and Evaluation of The Slim Fly Network}}, 
    author = {Nils Blach and Maciej Besta and Daniele De Sensi and Jens Domke and Hussein Harake and Shigang Li and Patrick Iff and Marek Konieczny and Kartik Lakhotia and Ales Kubicek and Marcel Ferrari and Fabrizio Petrini and Torsten Hoefler},
    year = 2023,
    eprinttype = {arXiv},
    eprint = {2310.03742}
}
```

## Original README Content
The original README content for OpenSM can be found [here](https://github.com/linux-rdma/opensm/blob/master/README).

## Feedback & Contributions
We welcome feedback on the LNMP algorithm, its implementation, and our paper.
For any queries, reach out to [nils.blach@inf.ethz.ch](mailto:nils.blach@inf.ethz.ch) or open an issue.