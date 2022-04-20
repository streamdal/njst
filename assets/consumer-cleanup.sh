for stream in `nats stream ls | grep njst | awk {'print $2'}`; do
	for consumer in `nats consumer ls $stream 2>&1 | grep njst-cg | awk {'print $1'}`; do
		echo "Removing consumer group $consumer for stream $stream..."
		nats consumer rm $stream $consumer -f
	done
done
