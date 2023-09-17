#!/bin/bash

# I think there's a kinda memory leak in the program at the moment,
# so we just end the process and start it again as needed
while true
do
    python -m server.algos.score_task
    sleep 60
    # PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python -m scripts.cv_images
done
