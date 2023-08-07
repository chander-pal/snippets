#In case we have to execute any comand like library version upgrade, 
#createing direcories etc on all nodes of EMR cluster, we can use this script

yarn node -list 2>/dev/null \
    | sed -n "s/^\(ip[^:]*\):.*/\1/p" \
    | xargs -t -I{} \
    ssh -i key.pem hadoop@{} \
    "sudo python3 -m pip install numpy --upgrade --ignore-installed"

# This script will upgrade numpy library on all nodes of EMR cluster

#lets say we need to create a folder on all nodes of EMR cluster, we can use below script
yarn node -list 2>/dev/null \
    | sed -n "s/^\(ip[^:]*\):.*/\1/p" \
    | xargs -t -I{} \
    ssh -i key.pem hadoop@{} \
    "sudo mkdir /home/hadoop/test"