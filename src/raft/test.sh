#!/bin/zsh

#sh ./go-test-many.sh 1000 4 2A

rm -rf *.log *.err
rm -rf logs

mkdir logs
nohup ./go-test-many.sh 5000 16 2A > ./logs/2a.log 2>&1 &
nohup ./go-test-many.sh 5000 16 2B > ./logs/2b.log 2>&1 &
nohup ./go-test-many.sh 5000 16 2C > ./logs/2c.log 2>&1 &
nohup ./go-test-many.sh 5000 16 2D > ./logs/2d.log 2>&1 &