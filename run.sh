#!/bin/sh
spark-submit \
  --class org.example.MainApp \
  --master local[4] \
  target/spark-exercise2-1.0-SNAPSHOT.jar target/resources/u.data target/resources/u.item output.json