#!/bin/bash

source ./functions.sh

case "$1" in
  start)
        start $2 $3 "$4"
        ;;
  stop)
        stop $2
        ;;
  kill)
        do_kill $2
        ;;
  reinstall)
        reinstall $2
        ;;
  rebuild)
        rebuild $2
        ;;
  status)
        status $2
        ;;
  cleanlogs)
        cleanlogs $2
        ;;
  taillogs)
        taillogs $2
        ;;
  createshard)
        createshard $2 $3 $4 $5
        ;;
  *)
        echo $"Usage: $0 { rebuild| reinstall <instanceid>| start <instanceid> [numshards]| stop <instanceid>|kill <instanceid>| status<instanceid>| cleanlogs<instanceid>| createshard <instance> <collection> <coreName> [shardId]}"
        exit 1
esac
exit 0