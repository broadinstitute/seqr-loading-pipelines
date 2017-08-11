This README describes how to deploy elasticsearch + kibana either locally or on gcloud using [Kubernetes](https://kubernetes.io/).

Prerequisites
-------------

The following dependencies must be installed on your laptop:

* python2.7
* [pip](https://pip.pypa.io/en/stable/installing/#installing-with-get-pip-py)
* a copy of this github repo

      git clone https://github.com/macarthur-lab/hail-db-utils.git 

* install this repo's python dependencies  
  
      cd hail-db-utils; pip install -r requirements.txt


**Local Dev. Instance on MacOSX**

The local installation relies on Kube-Solo (https://github.com/TheNewNormal/kube-solo-osx/releases) which is a low-overhead VM for running Kubernetes on a MacOSX laptop.

1. Install CoreOS, which is required for for Kub-Solo:

   a. `brew install libev`  
   b. Install the latest DMG from https://github.com/TheNewNormal/corectl.app/releases

   `WARNING: ` Being on a VPN connection may cause errors during CoreOS install steps that need to download components from the web.
   The solution is to disconnect from VPN.

2. Install Kube-Solo: https://github.com/TheNewNormal/kube-solo-osx/releases

3. Install kubectl: https://kubernetes.io/docs/tasks/kubectl/install/

4. Initialize:

   ![Kube-Solo](https://raw.githubusercontent.com/TheNewNormal/kube-solo-osx/master/kube-solo-osx.png "Kubernetes-Solo")

   a. When launching Kube-Solo for the 1st time, click on `Setup > Initial Setup of Kube-Solo VM`
      It will open an iTerm2 shell and ask for several inputs. The following settings are recommended:

         Set CoreOS Release Channel:         3) Stable (recommended)
         Please type VM's RAM size in GBs:   8
         Please type Data disk size in GBs:  20
 
   b. After this initial setup, you can just click `Preset OS Shell` to open a new terminal where docker and kubectl are preconfigured to use the local kubernetes cluster. 


5.  **Trouble-shooting Kube-Solo VM setup issues:** If your computer goes to sleep or reboots, the CoreOS / Kube-Solo VM may become unresponsive, requiring it to be rebooted (or possibly even reinitialized)

    For some reason,

            The following steps fail if you're connected to a VPN

    so be sure to disconnect before proceeding.

    You can click `Halt` and then `Up` in the Kube-Solo menu to shut-down and then restart the VM.
    This typically resolves most issues. If Halt takes a long time, running `pkill kube` on the command-line may help.
    Previously-deployed components will automatically start up when the VM restarts.

    If issues persist, you can delete and reinitialize the Kube-Solo VM by Halting it and then running `rm -rf ~/kube-solo`.
    If you then click `Up` in the Kube-Solo menu, it will reinitialize the VM from scratch.


**Production Instance on Google Cloud**

[Google Container Engine](https://cloud.google.com/container-engine/docs/) makes it easy to create a Kubernetes cluster and then deploy, manage, and scale an application. The following steps are necessary before `./servctl` can be used to deploy to a Google Container Engine cluster:

1. Install Docker for MacOSX:  
   https://docs.docker.com/docker-for-mac/install/

   It will be used to build docker images before pushing them to your private repo on Google Container Engine.

2. Install kubectl: https://kubernetes.io/docs/tasks/kubectl/install/


Configuration
-------------

`*-settings.yaml` - contain settings for local, dev and prod deployments.

Deployment
----------

To deploy all services to your Kubernetes cluster, run:

    ./servctl deploy {label}   # label can be 'local', 'gcloud-dev'


The `./servctl` script also provides subcommands for performing common steps for managing and troubleshooting:
         
      deploy                        # Deploy one or more components
      logs                          # show logs for one or more components
      port-forward                  # start port-forwarding for service(s) running in the given component container(s), allowing connections via localhost
      connect-to                    # starts port-forwarding and shows logs
      shell                         # open a bash shell inside one of the component containers
      status                        # print status of all kubernetes and docker subsystems
      dashboard                     # open the kubernetes dasbhoard in a browser
      kill                          # removes pods and other entities of the give component - the opposite of deploy.
      delete                        # clears the given database - deleteing all records
      kill-and-delete-all           # kill and deletes all resources, components and data - reseting the kubernetes environment to as close to a clean slate as possible


Running
-------


Currently there are 2 example scripts for exporting data to elasticsearch:
- `export_gnomad_to_ES.py` - joins gnomad exomes and genomes into a structure that contains all info used in the gnomAD browser (runs on a subset of the full VDS's by default).
- `export_callset_to_ES.py` - loads a VEP-annotated VDS with genotypes, adds in clinvar, gnomad AFs, and other reference data, and then exports both site-level annotations and genotypes to elasticsearch as would be used by seqr.

Before running these,
a dataproc cluster must be created, which can be done by running:

    ./create_cluster_without_VEP.sh

or if you plan to also use the cluster for running VEP, then instead use:

    ./create_cluster.GRCh37.sh or ./create_cluster.GRCh38.sh

the `export_callset_to_ES.py` script expects the input VCF (or VDS) to be
VEP-annotated. If it's not, you can 1st run:

    ./submit.sh run_vep.py gs://<input path>  gs://<output path>

Then, to export to elasticsearch, find the elasticsearch *node* (rather than pod)
IP address by running `kubectl get nodes`, and then `kubectl describe node <node name> | grep Addresses`.
The node IP should be similar to "10.128.0.251". Once you have it, you can run:

    ./submit.sh export_gnomad_to_ES.py --host <node ip>

       or

    ./submit.sh export_callset_to_ES.py -g 38 --host <node ip>




Future improvements
-------------------

Things to improve:
- Rely more on Kubernetes labels when using kubectl to perform various operations on components
- Borrow from or switch to one of the open-source repos for deploying a sharded elasticsearch cluster on kubernetes:
https://github.com/pires/kubernetes-elasticsearch-cluster
https://www.youtube.com/watch?v=3HnV7NfgP6A
https://github.com/kubernetes/examples/tree/master/staging/elasticsearch


Kubernetes Resources
--------------------

- Official Kuberentes User Guide:  https://kubernetes.io/docs/user-guide/
- 15 Kubernetes Features in 15 Minutes: https://www.youtube.com/watch?v=o85VR90RGNQ
- Kubernetes: Up and Running: https://www.safaribooksonline.com/library/view/kubernetes-up-and/9781491935668/
- The Children's Illustrated Guide to Kubernetes: https://deis.com/blog/2016/kubernetes-illustrated-guide/

