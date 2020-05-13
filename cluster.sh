#! /bin/bash

cmd=$1

if [ $cmd = 'start' ]; then
# Start Leader
echo 'Starting leader'
nohup go run server.go 127.0.0.1:7777 :8888 1 raftleader >> ./raftleader.log 2>&1 &
echo 'Waiting for leader starting'
sleep 3
echo 'Leader started'

# Start Followers
echo 'Starting followers'
nohup go run server.go 127.0.0.1:6666 :9999 0 raftfollower1 :8888 >> ./raftfollower1.log 2>&1 &
nohup go run server.go 127.0.0.1:4444 :5555 0 raftfollower2 :8888 >> ./raftfollower2.log 2>&1 &
nohup go run server.go 127.0.0.1:2222 :3333 0 raftfollower3 :8888 >> ./raftfollower3.log 2>&1 &
nohup go run server.go 127.0.0.1:1111 :11111 0 raftfollower4 :8888 >> ./raftfollower4.log 2>&1 &
echo 'Waiting for followers starting'
sleep 3
echo 'Followers started'
fi

if [ $cmd = 'stop' ]; then
echo 'Stopping cluster'
ps aux | egrep '(raftleader|raftfollower)' | grep -v grep | awk '{print $2}' | xargs kill -9
echo 'Cluster stopped'
fi
