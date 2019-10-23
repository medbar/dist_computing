
trap "echo ' Ctrl-C'; kill -9 \$(jobs -p); exit " SIGINT

nodes_ip="localhost"
nodes_ports=(6000 6001)
file=test.1

mkdir -p log 2>/dev/null
nodes=""
for p in ${nodes_ports[*]} ; do
  echo "Start worker on port " $p
  python3 worker.py $nodes_ip $p &> log/worker.$p.log &
  nodes="$nodes $nodes_ip:$p"
  done


echo "Start Master with nodes " $nodes " and command " $file
python3 master.py --input  $file $nodes 2> log/master.log

#kill -2 $(jobs -p)
#wait
#jobs -p
echo "Done"
exit 0
